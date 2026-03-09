import { streamIteratorSymbol } from './symbol.js';

export { streamIteratorSymbol };

export const getStreamIterator = (obj) => {
  return obj[streamIteratorSymbol]?.() || obj[Symbol.iterator]?.();
};

let { freeze } = Object;

let waitForPromises = (globalThis.__stream_generator_waitForPromises__ ||= new WeakSet());
let buffers = new WeakMap();

let buildDeferred = () => {
  let resolve, reject;
  let promise = new Promise((...args) => {
    ({ 0: resolve, 1: reject } = args);
  });
  return { promise, resolve, reject };
};

export const wait = (promise) => {
  waitForPromises.add(promise);
  return promise;
};

class Queue {
  constructor() {
    this.values = [];
    this.size = 0;
    this.start = 0;
  }

  at(idx) {
    let { start, values } = this;
    return values[(start + idx) % values.length];
  }

  push(value) {
    let { start, values } = this;
    this.size++;
    if (this.size > values.length) {
      values.push(value);
    } else {
      values[(start + this.size - 1) % values.length] = value;
    }
  }

  shift() {
    let value = this.at(0);
    this.size--;
    this.start = (this.start + 1) % this.values.length;
    return value;
  }
}

export class SyncGenerator {
  constructor(generator) {
    if (!generator.next) throw new Error();

    this.generator = generator;
    freeze(this);
  }

  next(value) {
    const step = this.generator.next(value);

    if (step instanceof Promise) {
      throw new Error('invalid embedded generator');
    }

    if (step.value instanceof Promise) {
      throw new Error('sync generators cannot resolve promises');
    } else {
      return step;
    }
  }

  return(value) {
    const step = this.generator.return(value);
    if (step instanceof Promise) {
      throw new Error('invalid embedded generator');
    }

    if (step.value instanceof Promise) {
      throw new Error('sync generators cannot resolve promises');
    }
    return step;
  }

  [Symbol.iterator]() {
    return this;
  }
}

export class AsyncGenerator {
  constructor(generator) {
    if (!generator.next) throw new Error();

    buffers.set(this, new Queue());

    this.generator = generator;
    freeze(this);
  }

  next(value) {
    let buffer = buffers.get(this);
    let deferred = buildDeferred();

    let cb = (step) => {
      if (step instanceof Promise) {
        throw new Error('invalid embedded generator');
      }

      if (waitForPromises.has(step.value)) {
        waitForPromises.delete(step.value);

        step.value.then((returnValue) => {
          cb(this.generator.next(returnValue));
        });
      } else {
        let deferred = buffer.shift();
        deferred.resolve(step);
      }
    };

    buffer.push(deferred);

    if (buffer.size > 1) {
      buffer.at(buffer.size - 2).promise.then(() => {
        cb(this.generator.next(value));
      });
    } else {
      cb(this.generator.next(value));
    }

    return deferred.promise;
  }

  return(value) {
    return this.generator.return(value);
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

export class StreamGenerator {
  constructor(generator) {
    if (!generator.next) throw new Error();

    buffers.set(this, new Queue());

    this.generator = generator;
    freeze(this);
  }

  next(value) {
    let buffer = buffers.get(this);

    let cb = (step) => {
      if (step instanceof Promise) {
        throw new Error('invalid embedded generator');
      }

      if (waitForPromises.has(step.value)) {
        waitForPromises.delete(step.value);

        return step.value.then((returnValue) => {
          return cb(this.generator.next(returnValue));
        });
      } else {
        let deferred = buffer.shift();
        deferred?.resolve(step);
        return step;
      }
    };

    let last;
    if (buffer.size > 0 && (last = buffer.at(buffer.size - 2))) {
      buffer.push(buildDeferred());
      return last.promise.then(() => {
        return cb(this.generator.next(value));
      });
    } else {
      buffer.push(null);
      return cb(this.generator.next(value));
    }
  }

  return(value) {
    return this.generator.return(value);
  }

  [streamIteratorSymbol]() {
    return this;
  }
}

export class StreamIterable {
  constructor(iterable) {
    this.iterable = iterable;

    freeze(this);
  }

  [Symbol.iterator]() {
    return new SyncGenerator(this.iterable);
  }

  [Symbol.asyncIterator]() {
    return new AsyncGenerator(this.iterable);
  }

  [streamIteratorSymbol]() {
    return new StreamGenerator(this.iterable);
  }
}

export const streamify = (generator) => {
  let { name } = generator;
  return {
    [name]: (...args) => {
      return new StreamIterable(generator(...args));
    },
  }[name];
};
