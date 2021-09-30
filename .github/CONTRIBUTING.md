# Contributing to Hazelcast Go Client

Hazelcast welcomes contributions to Hazelcast Go Client project on Github.

## Contributing

1. **Fork** the repo on GitHub. Your repo is called *origin*.
2. Clone *origin*
3. Add github.com/hazelcast/hazelcast-go-client as *upstream*
```
git remote add upstream ...
```
4. Make changes
5. Open a Pull request
6. Someone from the Hazelcast team will review your contribution.

## Style Guideline

The following summary is based on [Effective Go](https://golang.org/doc/effective_go), [Go Wiki Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) and [Readability](https://talks.golang.org/2014/readability.slide).

### General

* There's no line length limit in Go. Do not break lines (especially function signatures) to fit them to an arbitrary line length.
* If a line gets very long, you may try to reduce its width by renaming variables/parameters.

### Naming

`lowerCamelCase`, `UpperCamelCase`

* Prefer to use short names, especially for local variables.`id` is a better local variable name than `correlationID`, if there are no other IDs in that function.
* Use camel case for identifiers. Local identifiers should be lower camel-cased (but it's better to keep them a single word if possible)
* Import aliases should be all lowercased, without underscores.
    * Do not use well-known identifier names (such as `err`) as import aliases.
    * The import alias should make sense when read, do not use something like: `controller4`.
* Constants should be camel-cased, unlike C and Java.
* Receiver names should be just a few characters, ideally 1 or 2.

### Errors

1. DO NOT ignore errors, even in tests.
2. DO document why an error is ignored (ignoring errors is OK only in some rare cases).
3. DO NOT panic in a user-facing API.
4. DO NOT use panics as an error propagation mechanism. Returning an `error` is almost always the correct way to do that. It is acceptable to use panics in internals as a way to quickly get out of a deep call hierarchy.
5. If a function is returning an error, then it must be the last parameter.

### Functions and Methods

* If a method converts one type to another, use the type name directly. E.g., `String() string` not `ToString() string`.

### Receivers

There is not a "one-true way" of deciding between pointer and non-pointer receivers but here are a few rules that may help.

Remember that when using non-pointer receivers, the underlying value is copied and when using pointer receivers, the pointer to the underlying value is copied.

If a value implements an interface using a pointer receiver, then you must use a reference to satisfy the interface. See the example below:

```go
type Updater interface {
	Update()
}

type MyType struct {
	now time.Time
}

func (m *MyType) Update() {
	m.now = time.Now()
}

func main() {
	var updater Updater
    // Using a reference to a MyType value.
    // Removing the reference causes a compile time error.
	updater = &MyType{}
	updater.Update()
}
``` 

1. DO NOT use a pointer receiver if the receiver type is `map`, `chan` or `func`. If the receiver type is slice, do not use a pointer receiver unless the method reslices or reallocates the slice.
2. DO use a pointer receiver if the method mutates the underlying value (except cases in 1).
3. DO use a pointer receiver if the underlying struct contains a field which should not be copied, such as `*sync.Mutex` or `*tls.Config`.
4. DO use a pointer receiver if the underlying struct is not small.
5. Consider using a non-pointer receiver if the underlying struct is small. That can reduce the amount of garbage created.
6. DO use a pointer receiver if the method calls another method with a pointer receiver of the same underlying value.

### Context

Most methods in Hazelcast Go Client take a `context.Context` parameter. That allows our users to cancel a request manually or after a timeout.

Context cancellation doesn't work automatically. Your code should observe `ctx.Done()` to know when the context was cancelled and act accordingly.

When a context is created with `context.WithCancel`, `context.WithDeadline` or `context.WithTimeout`, a second `cancel` function is returned. The `cancel` function must be called to release the resources of the context. The best way to do that is calling `defer cancel()` right after the context is created.

1. Context parameter should be the first and it must be named `ctx`.
2. Do not store context in a struct, but pass it as the first parameter to functions that accept it.