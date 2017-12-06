import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.temporal.ChronoUnit

// stooq downloads: https://stooq.com/db/h/
// https://stooq.com/q/d/l/?s=^spx&d1=20130501&d2=20171013&i=d
class Stooq {
    private static final Logger LOGGER = LoggerFactory.getLogger(Stooq.class)

    def getHistData(String symbol, String fromYYYYMMDD="", boolean needBar=true, boolean needVolume=false) {
        def cache = new File(System.getProperty('java.io.tmpdir'), "cache.${symbol}${fromYYYYMMDD}.stooq")
        def url = fromYYYYMMDD.length() > 0 ? "https://stooq.com/q/d/l/?s=$symbol&d1=$fromYYYYMMDD&d2=20991231&i=d" : "https://stooq.com/q/d/l/?s=$symbol&i=d"
        def csv = ""
        def epoch = LocalDate.ofEpochDay(0)

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
            def datelets = cols[0].split("-").collect {it.toInteger()}

            if (validBar && validVol) {
                bars << [date   : ChronoUnit.DAYS.between(epoch, LocalDate.of(datelets[0], datelets[1], datelets[2])) as long, // Date.parse('yyyy-MM-dd', cols[0]).time / 1000,
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

