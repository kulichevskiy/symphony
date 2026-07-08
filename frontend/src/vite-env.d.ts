/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Auth0 tenant domain; unset on local loopback (auth disabled). */
  readonly VITE_AUTH0_DOMAIN?: string;
  /** Auth0 SPA application client id; unset on local loopback (auth disabled). */
  readonly VITE_AUTH0_CLIENT_ID?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
