import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";

export interface EscalateConfig {
    enabled: boolean;
    escalateAfterSeconds: number;
    nudgeIntervalSeconds: number;
    stickyAwayMode: boolean;
    phone: string;
    line?: string;
    projectId?: string;
    projectSecret?: string;
    labelPrefix: string;
}

type ConfigResult = { config: EscalateConfig } | { error: string };

type RawConfig = Partial<EscalateConfig>;

const DEFAULTS = {
    enabled: true,
    escalateAfterSeconds: 120,
    nudgeIntervalSeconds: 1800,
    stickyAwayMode: true,
    labelPrefix: "omp",
} as const;

function configPaths(cwd: string, agentDir: string): string[] {
    const configured = process.env.OMP_PHOTON_ESCALATE_CONFIG;
    const paths: string[] = [];
    if (configured && isAbsolute(configured)) paths.push(configured);
    paths.push(join(cwd, ".omp", "photon-escalate.json"));
    paths.push(join(agentDir, "photon-escalate.json"));
    return paths;
}

function invalid(path: string, message: string): ConfigResult {
    return { error: `photon-escalate: ${path}: ${message}` };
}

export function loadConfig(cwd: string, agentDir = join(homedir(), ".omp", "agent")): ConfigResult {
    const path = configPaths(cwd, agentDir).find(existsSync);
    if (!path) return { error: "photon-escalate: no config file found" };

    let raw: unknown;
    try {
        raw = JSON.parse(readFileSync(path, "utf8"));
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return invalid(path, message);
    }

    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
        return invalid(path, "configuration must be a JSON object");
    }

    const value = raw as RawConfig;
    const enabled = value.enabled ?? DEFAULTS.enabled;
    const escalateAfterSeconds = value.escalateAfterSeconds ?? DEFAULTS.escalateAfterSeconds;
    const nudgeIntervalSeconds = value.nudgeIntervalSeconds ?? DEFAULTS.nudgeIntervalSeconds;
    const stickyAwayMode = value.stickyAwayMode ?? DEFAULTS.stickyAwayMode;
    const labelPrefix = value.labelPrefix ?? DEFAULTS.labelPrefix;

    if (typeof enabled !== "boolean") return invalid(path, "'enabled' must be a boolean");
    if (
        typeof escalateAfterSeconds !== "number" ||
        !Number.isFinite(escalateAfterSeconds) ||
        escalateAfterSeconds < 0
    ) {
        return invalid(path, "'escalateAfterSeconds' must be a non-negative number");
    }
    if (
        typeof nudgeIntervalSeconds !== "number" ||
        !Number.isFinite(nudgeIntervalSeconds) ||
        nudgeIntervalSeconds < 0
    ) {
        return invalid(path, "'nudgeIntervalSeconds' must be a non-negative number");
    }
    if (typeof stickyAwayMode !== "boolean")
        return invalid(path, "'stickyAwayMode' must be a boolean");
    if (typeof labelPrefix !== "string" || labelPrefix.trim() === "") {
        return invalid(path, "'labelPrefix' must be a non-empty string");
    }

    const phone = typeof value.phone === "string" ? value.phone.trim() : "";
    if (!phone) {
        return { error: "photon-escalate: 'phone' is required (E.164 number or iMessage email)" };
    }

    const projectId = value.projectId || process.env.SPECTRUM_PROJECT_ID;
    const projectSecret = value.projectSecret || process.env.SPECTRUM_PROJECT_SECRET;
    if (!projectId || !projectSecret) {
        return {
            error: "photon-escalate: projectId/projectSecret missing (config or SPECTRUM_PROJECT_ID/SPECTRUM_PROJECT_SECRET)",
        };
    }

    if (value.line !== undefined && (typeof value.line !== "string" || value.line.trim() === "")) {
        return invalid(path, "'line' must be a non-empty string when provided");
    }
    if (typeof projectId !== "string" || typeof projectSecret !== "string") {
        return invalid(path, "'projectId' and 'projectSecret' must be strings");
    }

    return {
        config: {
            enabled,
            escalateAfterSeconds,
            nudgeIntervalSeconds,
            stickyAwayMode,
            phone,
            ...(value.line ? { line: value.line.trim() } : {}),
            projectId,
            projectSecret,
            labelPrefix: labelPrefix.trim(),
        },
    };
}
