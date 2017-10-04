import org.slf4j.Logger
import org.slf4j.LoggerFactory

// stooq downloads: https://stooq.com/db/h/
class Stooq {
    private static final Logger LOGGER = LoggerFactory.getLogger(Stooq.class)

    def getHistData(symbol, needBar=true, needVolume=false) {
        def cache = new File(System.getProperty('java.io.tmpdir'), "cache.${symbol}.stooq")
        def url = "https://stooq.com/q/d/l/?s=$symbol&i=d"
        def csv = ""

        // cache file for 8 hours
        if (cache.exists() && cache.lastModified() + 1000 * 60 * 8 >= System.currentTimeMillis()) {
            LOGGER.info("use cache for URL: {}", url)
            csv = cache.text
        } else {
            LOGGER.info("download: {}", url)
            csv = new URL(url).getText()
            cache.text = csv
        }

        def csvRows = csv.split("\n")
        if (csvRows.size() <= 1) return [[:]]

        def bars = []
        for (int i = 1; i < csvRows.size(); i++) {
            def cols = csvRows[i].split("\\s*,\\s*")
            def notBar = cols[1].toDouble() == cols[2].toDouble() == cols[3].toDouble() == cols[4].toDouble()
            def validBar = !needBar || !notBar
            def validVol = !needVolume || cols.size() >= 6

            if (validBar && validVol) {
                bars << [date   : Date.parse('yyyy-MM-dd', cols[0]).time,
                         symbol : symbol,
                         open   : cols[1].toDouble(),
                         high   : cols[2].toDouble(),
                         low    : cols[3].toDouble(),
                         close  : cols[4].toDouble(),
                         volume : cols.size() > 5 ? cols[5].toDouble() : 0d]
            }
        }

        return bars
    }

}

