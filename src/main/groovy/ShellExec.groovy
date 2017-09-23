import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ShellExec {
    private static final Logger logger = LoggerFactory.getLogger(ShellExec.class);

    static byte[] exec(Collection<String> commandlets) {
        def cmd = []

        if (SystemUtils.IS_OS_WINDOWS) {
            cmd = ["cmd", "/C"] + commandlets
        } else {
            cmd = commandlets
        }

        logger.info("execute: $cmd")
        def process = cmd.execute()
        def out = new ByteArrayOutputStream()
        def err = new ByteArrayOutputStream()
        process.waitForProcessOutput(out, err)

        if (process.exitValue() == 0) {
            def stdOut = out.toString()
            logger.info("${process.exitValue()}: ${stdOut.substring(0, Math.min(stdOut.length(), 255))}")
            return out.toByteArray()
        } else {
            def stdOut = out.toString()
            def errOut = err.toString()
            logger.error("${process.exitValue()}: ${stdOut}\n$errOut")
            return new byte[0]
        }
    }

}
