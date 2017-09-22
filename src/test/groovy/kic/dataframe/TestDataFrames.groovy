package kic.dataframe

/**
 * Created by kindler on 17/09/2017.
 */
class TestDataFrames {
    def columns = ["A", "B", "C", "D"]
    def rows = [0L, 1L, 2L, 3L, 4L, 5L]

    DataFrame integers(){
        DataFrame df  = new DataFrame()

        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < columns.size(); c++) {
                if (r != 4 || c != 3) {
                    df.upsert(rows[r], columns[c], r * 10L + c)
                }
            }
        }

        return df
    }

    DataFrame symetricDoubles() {
        String[] names = [ "O1", "O2", "O3", "O4", "O5", "O6" ]
        double[][] distances = [
            [ 0, 1, 9, 7, 11, 14 ],
            [ 1, 0, 4, 3, 8, 10 ],
            [ 9, 4, 0, 9, 2, 8 ],
            [ 7, 3, 9, 0, 6, 13 ],
            [ 11, 8, 2, 6, 0, 10 ],
            [ 14, 10, 8, 13, 10, 0 ]]
        DataFrame df = new DataFrame()
        for (int i = 0; i < names.length; i++) {
            for (int j = 0; j < names.length; j++) {
                df.upsert(names[i], names[j], distances[i][j])
            }
        }
        return df
    }

    DataFrame tallDoubles() {
        DataFrame df = new DataFrame()
        def columns = ["A", "B", "C"]
        def rows = [1L, 2L, 3L, 4L, 5L]
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < columns.size(); c++) {
                df.upsert(rows[r], columns[c], r + c / 2d)
            }
        }
        return df
    }
}
