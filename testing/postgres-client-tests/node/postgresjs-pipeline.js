import net from "node:net";
import postgres from "postgres";
import { getConfig } from "./helpers.js";

const SSL_REQUEST_CODE = 80877103;

class PgPipelineProbeProxy {
  constructor({ host, port }) {
    this.host = host;
    this.targetPort = Number(port);
    this.server = net.createServer((client) => this.handleClient(client));
    this.connections = new Set();
    this.armed = false;
    this.holdingServerResponses = false;
    this.pipelineProven = false;
    this.executionMessagesBeforeRelease = 0;
    this.waiters = [];
  }

  listen() {
    return new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(0, "127.0.0.1", () => {
        this.server.off("error", reject);
        this.port = this.server.address().port;
        resolve();
      });
    });
  }

  arm() {
    this.armed = true;
    this.holdingServerResponses = false;
    this.pipelineProven = false;
    this.executionMessagesBeforeRelease = 0;
    this.waiters = [];
    for (const connection of this.connections) {
      connection.heldServerData = [];
    }
  }

  waitForPipeline(timeoutMs) {
    if (this.pipelineProven) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(
          new Error(
            `postgres-js did not pipeline a second frontend execution within ${timeoutMs}ms`,
          ),
        );
      }, timeoutMs);
      this.waiters.push({
        resolve: () => {
          clearTimeout(timeout);
          resolve();
        },
        reject,
      });
    });
  }

  close() {
    for (const connection of this.connections) {
      connection.client.destroy();
      connection.upstream.destroy();
    }
    this.server.close();
  }

  handleClient(client) {
    const upstream = net.createConnection({
      host: this.host,
      port: this.targetPort,
    });
    const connection = {
      client,
      upstream,
      frontendBuffer: Buffer.alloc(0),
      expectingSSLResponse: false,
      startupDone: false,
      heldServerData: [],
    };
    this.connections.add(connection);

    client.on("data", (data) => {
      this.parseFrontendMessages(connection, data);
      upstream.write(data);
    });
    upstream.on("data", (data) => this.handleServerData(connection, data));

    const cleanup = () => {
      this.connections.delete(connection);
    };
    client.on("close", cleanup);
    upstream.on("close", cleanup);
    client.on("error", () => upstream.destroy());
    upstream.on("error", () => client.destroy());
  }

  parseFrontendMessages(connection, data) {
    connection.frontendBuffer = Buffer.concat([connection.frontendBuffer, data]);
    for (;;) {
      if (!connection.startupDone) {
        if (connection.frontendBuffer.length < 4) {
          return;
        }
        const length = connection.frontendBuffer.readUInt32BE(0);
        if (connection.frontendBuffer.length < length) {
          return;
        }
        const code = connection.frontendBuffer.readUInt32BE(4);
        connection.frontendBuffer = connection.frontendBuffer.subarray(length);
        if (length === 8 && code === SSL_REQUEST_CODE) {
          connection.expectingSSLResponse = true;
          continue;
        }
        connection.startupDone = true;
        continue;
      }

      if (connection.frontendBuffer.length < 5) {
        return;
      }
      const type = String.fromCharCode(connection.frontendBuffer[0]);
      const length = connection.frontendBuffer.readUInt32BE(1);
      const totalLength = 1 + length;
      if (connection.frontendBuffer.length < totalLength) {
        return;
      }
      connection.frontendBuffer = connection.frontendBuffer.subarray(totalLength);
      if (type === "Q" || type === "E") {
        this.recordFrontendExecution();
      }
    }
  }

  handleServerData(connection, data) {
    if (connection.expectingSSLResponse) {
      connection.client.write(data.subarray(0, 1));
      data = data.subarray(1);
      connection.expectingSSLResponse = false;
      if (data.length === 0) {
        return;
      }
    }

    if (this.holdingServerResponses && !this.pipelineProven) {
      connection.heldServerData.push(data);
      return;
    }
    connection.client.write(data);
  }

  recordFrontendExecution() {
    if (!this.armed || this.pipelineProven) {
      return;
    }
    this.executionMessagesBeforeRelease++;
    if (this.executionMessagesBeforeRelease === 1) {
      this.holdingServerResponses = true;
      return;
    }
    this.pipelineProven = true;
    this.releaseHeldServerData();
    for (const waiter of this.waiters) {
      waiter.resolve();
    }
  }

  releaseHeldServerData() {
    this.holdingServerResponses = false;
    for (const connection of this.connections) {
      for (const chunk of connection.heldServerData) {
        connection.client.write(chunk);
      }
      connection.heldServerData = [];
    }
  }
}

async function main() {
  const config = getConfig();
  const proxy = new PgPipelineProbeProxy({
    host: config.host,
    port: config.port,
  });
  await proxy.listen();

  const sql = postgres({
    host: "127.0.0.1",
    port: proxy.port,
    database: config.database,
    username: config.user,
    password: config.password,
    max: 1,
    ssl: false,
    prepare: false,
    connect_timeout: 5,
    idle_timeout: 1,
  });

  try {
    await sql`select 1 as warmup`;

    let pipelineProof;
    let resolveProbeStarted;
    const probeStarted = new Promise((resolve) => {
      resolveProbeStarted = resolve;
    });
    const transaction = Promise.resolve(sql.begin((tx) => {
      proxy.arm();
      pipelineProof = proxy.waitForPipeline(1000);
      resolveProbeStarted();
      return [
        tx`select 0::int as value`,
        tx`select 1::int as value`,
        tx`select 2::int as value`,
        tx`select 3::int as value`,
      ];
    }));
    await Promise.race([
      probeStarted,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("postgres-js transaction callback did not start")), 1000),
      ),
    ]);
    await pipelineProof;
    console.log(
      `postgres-js pipelined ${proxy.executionMessagesBeforeRelease} frontend executions before the first held response was released`,
    );
    const results = await transaction;
    const values = results.map((rows) => Number(rows[0].value)).sort();
    if (values.join(",") !== "0,1,2,3") {
      throw new Error(`unexpected postgres-js query results: ${values.join(",")}`);
    }
    if (proxy.executionMessagesBeforeRelease < 2) {
      throw new Error("postgres-js did not send multiple frontend executions before the first response");
    }
    console.log("postgres-js pipelined query results matched");
  } finally {
    proxy.releaseHeldServerData();
    await sql.end({ timeout: 1 });
    proxy.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
