import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';
import sitemap from '@astrojs/sitemap'

export default defineConfig({
  site: 'https://multipliedtwice.github.io',
  base: '/prisma-to-sql',
  outDir: '../docs',
  integrations: [tailwind(), sitemap()],
  i18n: {
    defaultLocale: 'en',
    locales: ['ar', 'bn', 'en', 'es', 'fr', 'hi', 'pt', 'ru', 'ur', 'zh'],
    routing: {
      prefixDefaultLocale: false
    }
  }
});