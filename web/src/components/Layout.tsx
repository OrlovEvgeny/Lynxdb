import { ComponentChildren } from "preact";
import { useRouter } from "preact-router";
import { LogoutButton } from "./AuthGate";
import styles from "./Layout.module.css";

interface Props {
  children: ComponentChildren;
}

export function Layout({ children }: Props) {
  const [routerState] = useRouter();
  const url = routerState?.url ?? "/";

  return (
    <div class={styles.layout}>
      <header class={styles.topbar}>
        <a href="/" class={styles.logo}>
          <span class={styles.logoMark}>&#9656;</span> LynxDB
        </a>
        <nav class={styles.navLinks}>
          <a href="/" class={url === "/" ? styles.active : undefined}>
            Search
          </a>
          <a
            href="/status"
            class={url === "/status" ? styles.active : undefined}
          >
            Status
          </a>
          <LogoutButton />
        </nav>
      </header>
      <main class={styles.content}>{children}</main>
    </div>
  );
}
