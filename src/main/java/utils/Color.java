package utils;

public class Color {
    public static final String SUCCESS = "\u001B[32m"; // Verde
    public static final String ERROR = "\u001B[31m"; // Vermelho
    public static final String WARNING = "\u001B[33m"; // Amarelo

    public static final String INFO = "\u001B[38;5;189m"; // Roxo claro
    public static final String MENU_OPTION = "\u001B[38;5;141m"; // Roxo médio
    public static final String HIGHLIGHT = "\u001B[38;5;99m"; // Roxo escuro
    public static final String TITLE = "\u001B[38;5;55m"; // Roxo profundo
    public static final String INPUT = "\u001B[38;5;147m"; // Roxo claro

    public static final String HEADER_BG = "\u001B[48;5;55m"; // Fundo escuro
    public static final String MENU_BG = "\u001B[48;5;141m"; // Fundo médio

    public static final String WHITE = "\u001B[37m";
    public static final String RESET = "\u001B[0m";

    public static String successMessage(String message) {
        return SUCCESS + message + RESET;
    }

    public static String errorMessage(String message) {
        return ERROR + message + RESET;
    }

    public static String warningMessage(String message) {
        return WARNING + message + RESET;
    }

    public static String infoMessage(String message) {
        return INFO + message + RESET;
    }

    public static String inputPrompt(String prompt) {
        return INPUT + prompt + RESET;
    }

    public static String title(String title) {
        return TITLE + title + RESET;
    }

    public static String header(String header) {
        return HEADER_BG + WHITE + header + RESET;
    }

    public static String menuOption(String option) {
        return MENU_OPTION + option + RESET;
    }

    public static String highlight(String text) {
        return HIGHLIGHT + text + RESET;
    }
}