import * as restate from "@restatedev/restate-sdk";

export type HandlerOpts<I = any, O = any> = {
    handlerName: string,
    inputSerde: restate.Serde<I>,
    outputSerde: restate.Serde<O>,
}

export function decorateVirtualObjectHandlers<P extends string, M>(
    vo: restate.VirtualObjectDefinition<P, M>,
    exclusiveDecorator: (opts: HandlerOpts, handler: (ctx: restate.ObjectContext, arg?: any) => Promise<any>) => (ctx: restate.ObjectContext, arg?: any) => Promise<any>,
    sharedDecorator: (opts: HandlerOpts, handler: (ctx: restate.ObjectSharedContext, arg?: any) => Promise<any>) => (ctx: restate.ObjectSharedContext, arg?: any) => Promise<any>
) {
    const handlers = (vo as any).object;

    for (const name in handlers) {
        // the hander you see is mainly a facade to make testign and type sigs easier
        // the actual hander that gets invoked is wrapped in a HandlerWrapper, which
        // handles headers, serde, etc.
        const handlerSurface = handlers[name];

        // the wrapper is currently et under a private symbol, so this
        // is a bit of a hack here to access it. this should be replaces by
        // some SDK tooling in the long run
        let handlerWrapper: any = undefined;
        for (const sym of Object.getOwnPropertySymbols(handlerSurface)) {
            if (sym.description === "Handler") {
                handlerWrapper = handlerSurface[sym];
            }
        }
        if (handlerWrapper === undefined) {
            throw new Error("Cannot access internal handler wrapper");
        }

        // depending on whether these are shared or exclusive handlers,
        // decorate them accordingly
        const originalHandler = handlerWrapper.handler;
        if (handlerWrapper.kind === 1) {
            handlerWrapper.handler = exclusiveDecorator(
                {
                    handlerName: name,
                    inputSerde: handlerWrapper.inputSerde,
                    outputSerde: handlerWrapper.outputSerde,
                },
                originalHandler
            );
        } else if (handlerWrapper.kind === 2) {
            handlerWrapper.handler = sharedDecorator( 
                {
                    handlerName: name,
                    inputSerde: handlerWrapper.inputSerde,
                    outputSerde: handlerWrapper.outputSerde,
                },
                originalHandler
            );
        } else {
            throw new Error("Unrecognized type of handler: " + handlerWrapper.kind);
        }
    }
}
