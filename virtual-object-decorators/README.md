# Virtual Object Decorators

A set of tools that 
A set of decorators for Virtual Objects, that add additional functionality.

## Archiving Virtual Object

If you store a lot of state in a Virtual Objects and want cold objects to be stored somewhere
else instead (like in an object store or a database), the [archiver](./src/archiver/) will write out VO state after it has not been modified for a specified duration.

The [app.ts](./src/archiver/app.ts) file shows an example of how to use this.
Run `npm run archiver` to start this demo service.

Below is a minimal pattern:
```TypeScript
const myVO = restate.object({ ... });

const archiving = archivingVirtualObject(
    myVO,
    {
        expiryTimeMs: 1000,
        writer: writeToS3(),
        reader: readFromS3()
    }
);
```
