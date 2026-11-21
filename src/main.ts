// The clock as a stdio MCP server. State in $CLOCK_STATE, else clock.json in its folder.
import path from "node:path"
import { serveStdio } from "switch-ext-kit"

import { clockApp, Clocks } from "./app.ts"

await serveStdio(clockApp, Clocks.layer(process.env.CLOCK_STATE ?? path.resolve("clock.json")))
