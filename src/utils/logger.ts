import * as pcModule from "picocolors";

const pc = (pcModule as any).default ?? pcModule;

function shouldDisableColor(): boolean {
  if ("NO_COLOR" in process.env) {
    return true;
  }

  const flag = process.env.NOMAD_NO_COLOR;
  if (flag === undefined) {
    return false;
  }

  const normalized = flag.trim().toLowerCase();
  if (normalized === "" || normalized === "1" || normalized === "true" || normalized === "yes") {
    return true;
  }

  return false;
}

const isColorSupported = Boolean(process.stdout?.isTTY) && !shouldDisableColor();

type ColorFn = (input: string) => string;

const magentaStrong: ColorFn = (input: string) => pc.bold(pc.magenta(input));
const greenCheck: ColorFn = (input: string) => pc.green(input);
const yellowWarn: ColorFn = (input: string) => pc.yellow(input);
const redError: ColorFn = (input: string) => pc.red(input);
const cyanInfo: ColorFn = (input: string) => pc.cyan(input);
const dimNote: ColorFn = (input: string) => pc.dim(input);

function paint(color: ColorFn, message: string): string {
  return isColorSupported ? color(message) : message;
}

function output(stream: "log" | "warn" | "error", message: string): void {
  // eslint-disable-next-line no-console
  console[stream](message);
}

export const logger = {
  action(message: string): void {
    output("log", paint(magentaStrong, `▶ ${message}`));
  },

  info(message: string): void {
    output("log", paint(cyanInfo, message));
  },

  success(message: string): void {
    output("log", paint(greenCheck, message));
  },

  warn(message: string): void {
    const formatted = message.startsWith("⚠") ? message : `⚠ ${message}`;
    output("warn", paint(yellowWarn, formatted));
  },

  error(message: string): void {
    output("error", paint(redError, message));
  },

  note(message: string): void {
    output("log", paint(dimNote, message));
  }
};

export function formatStrong(message: string): string {
  return paint(pc.bold, message);
}

export function formatPath(message: string): string {
  return paint(pc.magenta, message);
}
