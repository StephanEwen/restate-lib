import * as restate from "@restatedev/restate-sdk";
import { decorateVirtualObjectHandlers, HandlerOpts } from "../util/vo_decorator";

export function archivingVirtualObject<P extends string, M>(
    vo: restate.VirtualObjectDefinition<P, M>,
    opts: {
        expiryTimeMs: number,
        checkTimeMs?: number,
        writer: (obj: Record<string, any>) => Promise<string>,
        reader: (uri: string) => Promise<Record<string, any>>
    }
): restate.VirtualObjectDefinition<P, M> {
    const { expiryTimeMs, writer, reader } = opts;
    const checkTimeMs = opts.checkTimeMs ?? opts.expiryTimeMs / 2;
    const serviceName = vo.name;

    if (!expiryTimeMs || expiryTimeMs < 0) {
        throw new Error("Must have positive expiry time");
    }
    if (!checkTimeMs || checkTimeMs < 0) {
        throw new Error("Must have positive check time");
    }
    if (!reader || !writer) {
        throw new Error("Missing reader/writer");
    }

    // this object here defined the extra handlers needed.
    // we merge this into the main object
    const extraHandlersObject = restate.object({
        name: vo.name,
        handlers: {
            checkModTimeAndOffload: async (ctx: restate.ObjectContext): Promise<void> => {
                // check for stale timers, where the state was offloaded already
                if ((await ctx.get<string>(OFFLOAD_URL)) !== null) {
                    return;
                }
        
                // check for expiry time
                const lastMod = await ctx.get<number>(LAST_ACCESS);
                if (lastMod === null || Date.now() - lastMod < opts.expiryTimeMs) {
                    // reschedule the timer - we use genericsend here, because this is
                    // dynamic code that should work on any arbitrary VO
                    ctx.genericSend({
                        service: serviceName,
                        key: ctx.key,
                        method: "checkModTimeAndOffload",
                        parameter: undefined,
                        delay: checkTimeMs,
                        inputSerde: restate.serde.empty
                    });
                    return;
                }
        
                // offload state
                const state = {} as Record<string, any>
                for (const key of await ctx.stateKeys()) {
                    state[key] = await ctx.get(key);
                }
                delete state[LAST_ACCESS];

                let offloadUrl;
                try {
                    offloadUrl = await ctx.run("write state", () => opts.writer(state));
                } catch (e) {
                    if (e instanceof restate.TerminalError) {
                        // re-schedule timer - we use genericsend here, because this is
                        // dynamic code that should work on any arbitrary VO
                        ctx.genericSend({
                            service: serviceName,
                            key: ctx.key,
                            method: "checkModTimeAndOffload",
                            parameter: undefined,
                            delay: checkTimeMs,
                            inputSerde: restate.serde.empty
                        });
                    }
                    throw e;
                }

                ctx.clearAll();
                ctx.set(OFFLOAD_URL, offloadUrl);
            },
            
            loadStateAsync: async (ctx: restate.ObjectContext): Promise<void> => {
                const offloadUrl = await ctx.get<string>(OFFLOAD_URL);
                if (offloadUrl === null) {
                    // async load call my have been subsumed by different load. ignore.
                    return;
                }

                ctx.console.log("Loading back virtual object state...");
                const stateObj = await ctx.run(() => reader(offloadUrl));
                for (const key in stateObj) {
                    ctx.set(key, stateObj[key])
                }
                ctx.clear(OFFLOAD_URL);

                ctx.set(LAST_ACCESS, Date.now())
                // kick off a new expiry timer - we use genericsend here, because this is
                // dynamic code that should work on any arbitrary VO
                const thisKey = ctx.key;
                ctx.genericSend({
                    service: serviceName,
                    key: thisKey,
                    method: "checkModTimeAndOffload",
                    parameter: undefined,
                    delay: checkTimeMs,
                    inputSerde: restate.serde.empty
                });
            }
        }
    })
    
    const exclusiveDecorator = (_opts: HandlerOpts, originalFn: (ctx: restate.ObjectContext, arg?: any) => Promise<any>) => {
        return async (ctx: restate.ObjectContext, arg?: any): Promise<any> => {
            const offloadUrl = await ctx.get<string>(OFFLOAD_URL);
            if (offloadUrl !== null) {
                // this has been offloaded, load back
                ctx.console.log("Virtual Object state was offloaded, loading it back...");
                const stateObj = await ctx.run(() => reader(offloadUrl));
                for (const key in stateObj) {
                    ctx.set(key, stateObj[key])
                }
                ctx.clear(OFFLOAD_URL);
            }

            const result = await originalFn(ctx, arg);

            if ((await ctx.get(LAST_ACCESS)) === null) {
                // first time we are setting state, schedule a timer
                const thisKey = ctx.key;
                ctx.genericSend({
                    service: serviceName,
                    key: thisKey,
                    method: "checkModTimeAndOffload",
                    parameter: undefined,
                    delay: checkTimeMs,
                    inputSerde: restate.serde.empty
                });
            }
            ctx.set(LAST_ACCESS, Date.now())
            return result;
        }
    }

    const sharedDecorator = (opts: HandlerOpts, originalFn: (ctx: restate.ObjectSharedContext, arg?: any) => Promise<any>) => {
        return async (ctx: restate.ObjectSharedContext, arg?: any): Promise<any> => {
            if ((await ctx.get<string>(OFFLOAD_URL)) === null) {
                const result = await originalFn(ctx, arg);

                // at this time, shared hander access is not recorded as use and
                // will not prevent offloading. we can change that if needed
                return result;

            }

            // this has been offloaded, load back in a separate exclusive call
            ctx.console.log("Virtual Object state was found offloaded in shared handler. Triggering load in separate call...");
            await ctx.genericCall({
                service: serviceName,
                key: ctx.key,
                method: "loadStateAsync",
                parameter: undefined,
                inputSerde: restate.serde.empty
            });

            // because the view of state is by design locked during one durable execution,
            // we need to call ourselves to spawn a separate durable execution with the hydrated state view
            return await ctx.genericCall({
                service: serviceName,
                key: ctx.key,
                method: opts.handlerName,
                parameter: ctx.request().body,
                inputSerde: restate.serde.binary,
                outputSerde: opts.outputSerde
            });
        }
    }

    decorateVirtualObjectHandlers(vo, exclusiveDecorator, sharedDecorator);
    Object.assign((vo as any).object, (extraHandlersObject as any).object);

    return vo;
}

const LAST_ACCESS = "lastMod";
const OFFLOAD_URL = "offloadUrl";
