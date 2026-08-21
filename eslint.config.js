import eslint from "@eslint/js";
import { defineConfig } from "eslint/config";
import tseslint from "typescript-eslint";

export default defineConfig([
  {
    ignores: ["dist/", "results/"],
  },
  eslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: {
          allowDefaultProject: ["eslint.config.js"],
        },
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      "no-console": "off",
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        {
          allowNumber: true,
        },
      ],
    },
  },
]);
