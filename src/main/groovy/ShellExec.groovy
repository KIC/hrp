import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ShellExec {
    private static final Logger logger = LoggerFactory.getLogger(ShellExec.class);

    static String exec(String command) {
        def cmd = ""

        if (SystemUtils.IS_OS_WINDOWS) {
            cmd = "cmd /C $command"
        } else {
            cmd = "$command"
        }

        logger.info("execute: $cmd")
        def out = cmd.execute()
        out.waitFor()

        if (out.exitValue() == 0) {
            def stdOut = out.text
            logger.info("${out.exitValue()}: ${stdOut.substring(0, Math.min(stdOut.length(), 255))}")
            return stdOut
        } else {
            logger.error("${out.exitValue()}: ${out.text}")
            return ""
        }

    }
}
