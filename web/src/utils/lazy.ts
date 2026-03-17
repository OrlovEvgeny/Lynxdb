import { h, ComponentType } from "preact";
import { useState, useEffect } from "preact/hooks";

/**
 * Lightweight lazy-loading wrapper for Preact components using dynamic import().
 * Returns a component that renders null until the module loads, then re-renders
 * with the loaded component. The module is only fetched once (cached in closure).
 */
export function lazy<P>(load: () => Promise<{ default: ComponentType<P> }>) {
  let Comp: ComponentType<P> | null = null;
  let promise: Promise<void> | null = null;

  return function LazyComponent(props: P & { path?: string }) {
    const [, update] = useState(0);

    if (!Comp && !promise) {
      promise = load().then((m) => {
        Comp = m.default;
      });
    }

    useEffect(() => {
      if (!Comp && promise) {
        promise.then(() => update((n) => n + 1));
      }
    }, []);

    return Comp ? h(Comp, props) : null;
  };
}
