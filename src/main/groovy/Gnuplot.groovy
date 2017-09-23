import kic.dataframe.DataFrame

import java.util.function.Consumer
import java.util.function.Function

// runs the command: ShellExec.exec("gnuplot -e \"filename='${dataFile.toString()}'; weights=${joined.columns() + 1}\" \"${plotFile.toString()}\" > \"${imageFile.toString()}\"")
class Gnuplot implements Consumer<DataFrame> {
    final File plotFile
    final File imageFile
    final Function<DataFrame, Map> arguments = null

    Gnuplot(File plotFile, File imageFile, Function<DataFrame, Map> arguments = null) {
        this.plotFile = plotFile
        this.imageFile = imageFile
        this.arguments = arguments
    }

    @Override
    void accept(DataFrame dataFrame) {
        def args = arguments == null ? "" : arguments.apply(dataFrame).collect { k,v -> "$k=${"$v".isNumber() ? v : "'$v'"}" }.join("; ")
        def dataFile = new File(System.getProperty('java.io.tmpdir'), "${UUID.randomUUID().toString()}.csv")
        dataFile.text = dataFrame.toString().replaceAll("null", "0.0")
        def command = "gnuplot -e \"set output '${imageFile.toString()}'; filename='${dataFile.toString()}'; ${args}\" \"${plotFile.toString()}\""
        ShellExec.exec(command)
    }

}
