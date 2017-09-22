import org.apache.commons.lang3.SystemUtils

class ShellExec {
    static void exec(String command) {
        if (SystemUtils.IS_OS_WINDOWS) {
            println("cmd /C $command")
            "cmd /C $command".execute()
        } else {
            println(command)
            "$command".execute()
        }
    }
}
