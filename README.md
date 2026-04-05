# @bablr/stream-iterator

Defines a stream iterator, being an iterator object whose `next` method returns either `{ done, value }` or `Promise.resolve({ done, value })`. For more information on why this is (very) desirable see [the bablr docs](https://docs.bablr.org/architecture/synchronicity/).

## Usage

```js
import { streamIteratorSymbol } from '@bablr/stream-iterator';

// register defines this well-known symbol
Symbol.streamIterator === streamIteratorSymbol; // true
```
