import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://multipliedtwice.github.io',
  base: '/prisma-to-sql',
  outDir: '../docs',
  integrations: [tailwind()],
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'ru', 'zh'],
    routing: {
      prefixDefaultLocale: false
    }
  }
});