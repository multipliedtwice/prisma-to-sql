/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}',
    './src/**/*.astro',
  ],
  safelist: ["bg-gray-400"],
  theme: {
    extend: {
      colors: {
        primary: '#2D3748',
        accent: '#48BB78',
      },
    },
  },
  plugins: [],
}