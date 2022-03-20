package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 * col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
    int numCols;
    int numRows;
    ByteBuffer columns;

    public ColumnTable() {
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                this.columns.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columns.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columns.putInt(offset, field);
    }

    /**
     * Implements the query
     * SELECT SUM(col0) FROM table;
     * <p>
     * Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        int result = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            result += getIntField(rowId, 0);

        }
        return result;
    }

    /**
     * Implements the query
     * SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     * <p>
     * Returns the sum of all elements in the first column of the table,
     * subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        for (int i = 0; i < numRows; i++) {
            int val1 = getIntField(i, 1);
            int val2 = getIntField(i, 2);
            if (val1 > threshold1 && val2 < threshold2) {
                sum += getIntField(i, 0);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     * SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     * <p>
     * Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long result = 0;
        for (int i = 0; i < numRows; i++) {
            final int col0 = getIntField(i, 0);
            if (col0 > threshold) {
                result += col0;
                for (int j = 1; j < numCols; j++) {
                    result += getIntField(i, j);
                }
            }
        }
        return result;
    }

    /**
     * Implements the query
     * UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     * <p>
     * Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int count = 0;
        for (int i = 0; i < numRows; i++) {
            final int col0 = getIntField(i, 0);
            if (col0 < threshold) {
                count++;
            }
        }
        return count;
    }
}
