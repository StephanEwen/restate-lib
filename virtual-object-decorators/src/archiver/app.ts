import * as restate from "@restatedev/restate-sdk";
import { promises as fs } from "fs";
import { archivingVirtualObject } from "./archiving_vo";

const shared = restate.handlers.object.shared;

type State = {
  count: number,
  incCalled: number,
  decCalled: number
}

const myVO = restate.object({
  name: "count",
  handlers: {
    inc: async (ctx: restate.ObjectContext<State>) => {
      let count = (await ctx.get("count")) ?? 0;
      let inc = (await ctx.get("incCalled")) ?? 0;

      ctx.set("count", ++count);
      ctx.set("incCalled", ++inc);

      return { count, inc }
    },
    dec: async (ctx: restate.ObjectContext<State>) => {
      let count = (await ctx.get("count")) ?? 0;
      let dec = (await ctx.get("decCalled")) ?? 0;

      ctx.set("count", --count);
      ctx.set("decCalled", ++dec);

      return { count, dec }
    },
    count: shared(async (ctx: restate.ObjectSharedContext<State>) => {
      return (await ctx.get("count")) ?? 0;
    })
  },
})

const archiving = archivingVirtualObject(myVO, {
  expiryTimeMs: 1000,
  writer: writeToFile,
  reader: readFromFile
});

restate
  .endpoint()
  .bind(archiving)
  .listen(9080);


// ----------------------------------------------------------------------------
//  These are the custom functions to handle the storage for the  
// ----------------------------------------------------------------------------

async function readFromFile<T>(file: string): Promise<Record<string, any>> {
  try {
      const data = await fs.readFile(file, "utf8");
      return JSON.parse(data) as Record<string, any>;
  } catch (error: any) {
      if (error.code === "ENOENT") {
          throw new restate.TerminalError(`Could not read from file: ${file}. File does not exist.`);
      }
      throw error;
  }
}

async function writeToFile(obj: Record<string, any>): Promise<string> {
  const dir = "/tmp/swap-virtual-object";
  const file = `${dir}/${crypto.randomUUID()}`
  try {
      await fs.mkdir(dir, { recursive: true });
      const stateJson = JSON.stringify(obj);
      await fs.writeFile(file, stateJson);
      return file;
  } catch (error: any) {
      if (error.code === 'ENOENT') {
          throw new restate.TerminalError(`Could not write to file: ${file}. Path does not exist.`);
      }
      throw error;
  }
}
