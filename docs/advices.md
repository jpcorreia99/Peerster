# Advices and hints when using Go

The lab's guideline for go: https://dedis.github.io/dela/#/guideline

A lab's presentation about Go:
https://github.com/dedis/go-presentation/releases/tag/v0.0.1 (in Assets, see
`Go-for-your-PhD.pdf`)

## Errors

**Always** handle errors and wrap them. Always. It is ok and even sane if your
code is filled with:

```go
result, err := GetResult()
if err != nil {
  return xerrors.Errorf(“failed to get result: %v”, err)
}
```

In Go error messages should never start with a capital letter. Also, it is
common to use the following format for errors: `<error description>: <error>`.

## Logs

Logs can save you a lot of debugging time. We recommend using `zerolog`, a
simple but powerful logging library. You can take inspiration from the following
file to see how to initialize a logging instance:

https://github.com/dedis/dela/blob/6aaa2373492e8b5740c0a1eb88cf2bc7aa331ac0/mod.go#L59

Then, in your code, you can log messages with different logging level like:

```go
log.Info().Msgf("received a message from: %s", packet.Header.Source)
log.Warn().Msg("ack not received")
log.Err(err).Msg("failed to send message")
```

## UDP port

When creating an UDP connection, giving port 0 will make the system choose a
random free port for us. This is what we will use in tests with the UDP network.
Note that this means the provided address in the configuration can not be
assumed to be the definitive address of the node. The definitive address is only
known when the connection has been created. Until then, the address is
considered unknown. This is why the socket interface defines a GetAddress().

## Tests

When running your tests, don’t forget to include the `-race` flag. `-race` will
detect any race condition. `-run Test_XXX` can be used to target all tests
matching `Test_XXX`.

## Race conditions with maps

Access to a map shouldn’t be done concurrently. `sync.Mutex` is your best friend
there. It is recommended to create types for your map with thread-safe
functions. For example consider the following “doStuff” function:

```go
type Dummy struct {
    // ...
    myMap    map[int]int
    mapMutex sync.Mutex
}

func (t *Dummy) doStuff() {
    // ...
    // this is tedious to use
    t.mapMutex.Lock()
    t.myMap[0] = 0
    t.mapMutex.Unlock()
}
```

The previous example is tedious to use, as every time we need to access `myMap`
we have to think about using the mutex, as is the case in the `doStuff()`
function. It also overloads the `Dummy` struct with a `mapMutex`. Instead,
consider defining a new type that wraps the map with thread-safe functions, a
new `myMap` type: 

```go
type BetterDummy struct {
    // ...
    myMap myMap
}

func (t *BetterDummy) doStuff() {
    // ...
    // better: no need to think about mutex
    t.myMap.add(0, 0)
}

type myMap struct {
    sync.Mutex
    els map[int]int
}

func (m *myMap) add(key, val int) {
    m.Lock()
    defer m.Unlock()

    m.els[key] = val
} 
```
