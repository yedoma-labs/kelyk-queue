import { defineConfig } from "tsdown";

export default defineConfig([
	{
		entry: ["src/index.ts"],
		format: "esm",
		outDir: "dist",
		dts: true,
		clean: true,
		sourcemap: true,
		platform: "node",
		target: "es2022",
		outExtensions({ format }) {
			return { js: ".js", dts: ".d.ts" };
		},
	},
	{
		entry: ["src/index.ts"],
		format: "cjs",
		outDir: "dist",
		dts: true,
		clean: false,
		sourcemap: true,
		platform: "node",
		target: "es2022",
		outExtensions({ format }) {
			return { js: ".cjs", dts: ".d.cts" };
		},
	},
]);
