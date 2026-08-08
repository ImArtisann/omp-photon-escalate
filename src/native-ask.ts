import type {
    AgentToolResult,
    AskToolDetails,
    AskToolInput,
    ExtensionAPI,
    QuestionResult,
} from "@oh-my-pi/pi-coding-agent";

const OTHER_OPTION = "Other (type your own)";
const CHAT_ABOUT_THIS_OPTION = "Chat about this";
const NEXT_OPTION = "Next →";
const RESERVED_OPTION_LABELS: Record<string, true> = {
    [OTHER_OPTION]: true,
    [CHAT_ABOUT_THIS_OPTION]: true,
    [NEXT_OPTION]: true,
};

export function buildAskSchema(t: ExtensionAPI["arktype"]) {
    const OptionItem = t({
        label: t("string").describe("display label"),
        "description?": t("string").describe("optional explanatory text displayed below the label"),
        "preview?": t("string").describe(
            "optional rich preview content for interactive ask dialogs",
        ),
    });

    const QuestionItem = t({
        id: t("string").describe("question id"),
        question: t("string").describe("question text"),
        "header?": t("string").describe("optional short display chip for rich ask dialogs"),
        options: OptionItem.array().describe("available options"),
        "multi?": t("boolean").describe("allow multiple selections"),
        "recommended?": t("number").describe("recommended option index"),
    }).narrow((question, ctx) => {
        const reserved = question.options.find(
            (option) => RESERVED_OPTION_LABELS[option.label] === true,
        );
        return (
            reserved === undefined ||
            ctx.mustBe(
                `defined with option labels that do not collide with reserved runtime labels: ${reserved.label}`,
            )
        );
    });

    return t({
        questions: QuestionItem.array().atLeastLength(1).describe("questions to ask"),
    });
}

export const ASK_DESCRIPTION = `Asks user when you need clarification or input during task execution.

<conditions>
- Multiple approaches exist with significantly different tradeoffs user should weigh
</conditions>

<instruction>
- Use \`recommended: <index>\` to mark default (0-indexed); " (Recommended)" added automatically
- Use \`questions\` for multiple related questions instead of asking one at a time
- Set \`multi: true\` on question to allow multiple selections
- Use short option labels; put explanatory tradeoffs in \`description\` instead of merging them into the label
</instruction>

<caution>
- Provide 2-5 concise, distinct options
</caution>

<critical>
- **Default to action.** Resolve ambiguity yourself using repo conventions, existing patterns, and reasonable defaults. Exhaust existing sources (code, configs, docs, history) before asking. Only ask when options have materially different tradeoffs the user must decide.
- **If multiple choices are acceptable**, pick the most conservative/standard option and proceed; state the choice.
- **Do NOT include "Other" option** — UI automatically adds "Other (type your own)" to every question.
</critical>
`;

export function formatQuestionResult(result: QuestionResult): string {
    const noteSuffix = result.note ? ` (note: ${result.note})` : "";
    if (result.customInput !== undefined) {
        return `${result.id}: "${result.customInput}"${noteSuffix}`;
    }
    if (result.selectedOptions.length > 0) {
        const suffix = `${result.timedOut ? " (auto-selected after timeout)" : ""}${noteSuffix}`;
        return result.multi
            ? `${result.id}: [${result.selectedOptions.join(", ")}]${suffix}`
            : `${result.id}: ${result.selectedOptions[0]}${suffix}`;
    }
    return `${result.id}: (cancelled)${noteSuffix}`;
}

export function formatSingleQuestionResponse(result: {
    selectedOptions: string[];
    customInput?: string;
    note?: string;
    timedOut?: boolean;
    multi: boolean;
}): string {
    const responseParts: string[] = [];
    if (result.selectedOptions.length > 0) {
        const selectedText = result.multi
            ? `User selected: ${result.selectedOptions.join(", ")}`
            : `User selected: ${result.selectedOptions[0]}`;
        responseParts.push(
            result.timedOut ? `${selectedText} (auto-selected after timeout)` : selectedText,
        );
    }
    if (result.customInput !== undefined) {
        responseParts.push(
            result.customInput.includes("\n")
                ? `User provided custom input:\n${result.customInput
                      .split("\n")
                      .map((line) => `  ${line}`)
                      .join("\n")}`
                : `User provided custom input: ${result.customInput}`,
        );
    }
    if (result.note) {
        responseParts.push(
            result.note.includes("\n")
                ? `User added note:\n${result.note
                      .split("\n")
                      .map((line) => `  ${line}`)
                      .join("\n")}`
                : `User added note: ${result.note}`,
        );
    }
    return responseParts.length > 0 ? responseParts.join("\n") : "User cancelled the selection";
}

export function buildResult(
    questions: AskToolInput["questions"],
    results: QuestionResult[],
): AgentToolResult<AskToolDetails> {
    if (questions.length === 1) {
        const [question] = questions;
        const [result] = results;
        if (!question || !result)
            throw new Error("Phone answer did not match the requested question");

        const details: AskToolDetails = {
            question: question.question,
            options: question.options.map((option) => option.label),
            multi: question.multi ?? false,
            selectedOptions: result.selectedOptions,
            customInput: result.customInput,
            note: result.note,
            timedOut: result.timedOut,
        };
        const responseText = formatSingleQuestionResponse({
            selectedOptions: result.selectedOptions,
            customInput: result.customInput,
            note: result.note,
            timedOut: result.timedOut,
            multi: question.multi ?? false,
        });
        return { content: [{ type: "text", text: responseText }], details };
    }

    const details: AskToolDetails = { results };
    const responseLines = results.map(formatQuestionResult);
    const responseText = `User answers:\n${responseLines.join("\n")}`;
    return { content: [{ type: "text", text: responseText }], details };
}
