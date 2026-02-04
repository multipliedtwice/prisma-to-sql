interface Node<K, V> {
  key: K;
  value: V;
  freq: number;
  queue: "small" | "main";
  prev: Node<K, V> | null;
  next: Node<K, V> | null;
}

function withDispose<T>(it: IterableIterator<T>): MapIterator<T> {
  const anyIt = it as any;
  if (anyIt[Symbol.dispose] === undefined) {
    anyIt[Symbol.dispose] = () => {};
  }
  return it as unknown as MapIterator<T>;
}

export class BoundedCache<K, V> implements Map<K, V> {
  private map: Map<K, Node<K, V>> = new Map();
  private ghost: Set<K> = new Set();

  private smallHead: Node<K, V> | null = null;
  private smallTail: Node<K, V> | null = null;
  private smallSize = 0;

  private mainHead: Node<K, V> | null = null;
  private mainTail: Node<K, V> | null = null;
  private mainSize = 0;

  private readonly maxSize: number;
  private readonly smallLimit: number;
  private readonly mainLimit: number;
  private readonly ghostLimit: number;

  constructor(maxSize: number) {
    this.maxSize = maxSize;
    this.smallLimit = Math.max(1, Math.floor(maxSize * 0.1));
    this.mainLimit = maxSize - this.smallLimit;
    this.ghostLimit = this.mainLimit;
  }

  get size(): number {
    return this.map.size;
  }

  get(key: K): V | undefined {
    const node = this.map.get(key);
    if (!node) return undefined;

    node.freq = Math.min(node.freq + 1, 3);
    return node.value;
  }

  set(key: K, value: V): this {
    const existing = this.map.get(key);

    if (existing) {
      existing.value = value;
      return this;
    }

    if (this.ghost.has(key)) {
      this.ghost.delete(key);
      const node = this.createNode(key, value, "main");
      this.map.set(key, node);
      this.pushMain(node);

      if (this.mainSize > this.mainLimit) this.evictMain();
      return this;
    }

    const node = this.createNode(key, value, "small");
    this.map.set(key, node);
    this.pushSmall(node);

    if (this.size > this.maxSize) {
      if (this.smallSize > this.smallLimit) this.evictSmall();
      else this.evictMain();
    }

    return this;
  }

  has(key: K): boolean {
    return this.map.has(key);
  }

  delete(key: K): boolean {
    const node = this.map.get(key);
    if (!node) return false;

    this.map.delete(key);
    this.removeNode(node);
    return true;
  }

  clear(): void {
    this.map.clear();
    this.ghost.clear();
    this.smallHead = this.smallTail = null;
    this.mainHead = this.mainTail = null;
    this.smallSize = this.mainSize = 0;
  }

  keys(): MapIterator<K> {
    return withDispose(
      (function* (self: BoundedCache<K, V>) {
        for (const key of self.map.keys()) yield key;
      })(this),
    );
  }

  values(): MapIterator<V> {
    return withDispose(
      (function* (self: BoundedCache<K, V>) {
        for (const node of self.map.values()) yield node.value;
      })(this),
    );
  }

  entries(): MapIterator<[K, V]> {
    return withDispose(
      (function* (self: BoundedCache<K, V>) {
        for (const [key, node] of self.map.entries())
          yield [key, node.value] as [K, V];
      })(this),
    );
  }

  forEach(
    callbackfn: (value: V, key: K, map: Map<K, V>) => void,
    thisArg?: any,
  ): void {
    for (const [key, node] of this.map.entries()) {
      callbackfn.call(thisArg, node.value, key, this);
    }
  }

  [Symbol.iterator](): MapIterator<[K, V]> {
    return this.entries();
  }

  get [Symbol.toStringTag](): string {
    return "BoundedCache";
  }

  private createNode(key: K, value: V, queue: "small" | "main"): Node<K, V> {
    return { key, value, freq: 0, queue, prev: null, next: null };
  }

  private pushSmall(node: Node<K, V>): void {
    node.next = this.smallHead;
    node.prev = null;

    if (this.smallHead) this.smallHead.prev = node;
    else this.smallTail = node;

    this.smallHead = node;
    this.smallSize++;
  }

  private pushMain(node: Node<K, V>): void {
    node.next = this.mainHead;
    node.prev = null;

    if (this.mainHead) this.mainHead.prev = node;
    else this.mainTail = node;

    this.mainHead = node;
    this.mainSize++;
  }

  private popSmall(): Node<K, V> | null {
    if (!this.smallTail) return null;

    const node = this.smallTail;
    this.smallTail = node.prev;

    if (this.smallTail) this.smallTail.next = null;
    else this.smallHead = null;

    node.prev = null;
    node.next = null;
    this.smallSize--;
    return node;
  }

  private popMain(): Node<K, V> | null {
    if (!this.mainTail) return null;

    const node = this.mainTail;
    this.mainTail = node.prev;

    if (this.mainTail) this.mainTail.next = null;
    else this.mainHead = null;

    node.prev = null;
    node.next = null;
    this.mainSize--;
    return node;
  }

  private removeNode(node: Node<K, V>): void {
    this.unlinkNode(node);

    if (node.queue === "small") {
      if (node === this.smallHead) this.smallHead = node.next;
      if (node === this.smallTail) this.smallTail = node.prev;
      this.smallSize--;
    } else {
      if (node === this.mainHead) this.mainHead = node.next;
      if (node === this.mainTail) this.mainTail = node.prev;
      this.mainSize--;
    }

    node.prev = null;
    node.next = null;
  }

  private unlinkNode(node: Node<K, V>): void {
    if (node.prev) node.prev.next = node.next;
    if (node.next) node.next.prev = node.prev;
  }

  private shouldPromoteFromSmall(node: Node<K, V>): boolean {
    return node.freq > 1;
  }

  private shouldRetryInMain(node: Node<K, V>): boolean {
    return node.freq >= 1;
  }

  private promoteToMain(node: Node<K, V>): void {
    node.queue = "main";
    this.pushMain(node);
  }

  private addToGhost(key: K): void {
    this.ghost.add(key);

    if (this.ghost.size <= this.ghostLimit) return;

    const firstGhost = this.ghost.values().next().value;
    if (firstGhost !== undefined) this.ghost.delete(firstGhost);
  }

  private evictFromCache(node: Node<K, V>): void {
    this.map.delete(node.key);
  }

  private evictSmall(): void {
    while (this.smallSize > 0) {
      const node = this.popSmall();
      if (!node) return;

      if (this.shouldPromoteFromSmall(node)) {
        this.promoteToMain(node);

        if (this.mainSize > this.mainLimit) {
          this.evictMain();
          return;
        }

        continue;
      }

      this.evictFromCache(node);
      this.addToGhost(node.key);
      return;
    }
  }

  private evictMain(): void {
    while (this.mainSize > 0) {
      const node = this.popMain();
      if (!node) return;

      if (this.shouldRetryInMain(node)) {
        node.freq--;
        this.pushMain(node);
        continue;
      }

      this.evictFromCache(node);
      return;
    }
  }
}

export function createBoundedCache<K, V>(maxSize: number): Map<K, V> {
  return new BoundedCache<K, V>(maxSize);
}
