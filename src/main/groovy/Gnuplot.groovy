import kic.dataframe.DataFrame

import java.util.function.Consumer
import java.util.function.Function

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
        dataFile.text = dataFrame.map({formatNumber(it)} ).withDefault(formatNumber(0d)).toString()

        def command = ["gnuplot", "-e", "\"filename='${dataFile.toString()}';${args}\"", plotFile.toString()]
        imageFile.withOutputStream { it.write(ShellExec.exec(command)) }
    }

    String formatNumber(Object d) {
        return String.format("%.9f", d != null && d.toString().isNumber() ? d : 0d)
    }

}
