import { streamIteratorSymbol } from './symbol.js';

export { streamIteratorSymbol };

export const getStreamIterator = (obj) => {
  return obj[streamIteratorSymbol]?.() || obj[Symbol.iterator]?.();
};

let waitForPromises = new Set();

export const wait = (promise) => {
  waitForPromises.add(promise);
  return promise;
};

export class SyncGenerator {
  constructor(generator) {
    if (!generator.next) throw new Error();

    this.generator = generator;
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
    this.generator = generator;
  }

  next(value) {
    const step = this.generator.next(value);

    if (step instanceof Promise) {
      throw new Error('invalid embedded generator');
    }

    if (step.value instanceof Promise) {
      return step.value.then((value) => {
        return this.next(value);
      });
    } else {
      return Promise.resolve(step);
    }
  }

  return(value) {
    const result = this.generator.return(value);
    if (result instanceof Promise) {
      throw new Error('sync generators cannot resolve promises');
    }
    return result;
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

export class StreamGenerator {
  constructor(generator) {
    this.generator = generator;
  }

  next(value) {
    const step = this.generator.next(value);

    if (step.value instanceof Promise && waitForPromises.has(step.value)) {
      waitForPromises.delete(step.value);

      return step.value.then((value) => {
        return this.next(value);
      });
    } else {
      return step;
    }
  }

  return(value) {
    return this.generator.return(value);
  }

  [Symbol.iterator]() {
    return this;
  }

  [streamIteratorSymbol]() {
    return this;
  }
}

export class StreamIterable {
  constructor(iterable) {
    this.iterable = iterable;
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
