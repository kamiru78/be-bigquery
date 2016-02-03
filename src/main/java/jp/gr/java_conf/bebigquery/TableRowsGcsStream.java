package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class TableRowsGcsStream implements Iterator<TableRow>, Iterable<TableRow> {
    private final Pattern CVS_ELEMENT_PATTERN = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");
    private final Logger logger = LoggerFactory.getLogger(TableRowsGcsStream.class);
    private BufferedReader stream;
    private String nextLine;

    public TableRowsGcsStream(InputStream is) {
        stream = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public Iterator<TableRow> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        // at first
        if (nextLine == null) {
            fetch();
        }
        //
        return nextLine != null;
    }

    @Override
    public TableRow next() {
        String result = nextLine;

        logger.debug("result=" + result);
        List<TableCell> cellList = new ArrayList<TableCell>();
        Matcher matcher = CVS_ELEMENT_PATTERN.matcher(result);
        while (matcher.find()) {
            TableCell resultCell = new TableCell();
            resultCell.setV(matcher.group());
            cellList.add(resultCell);
        }
        TableRow resultRow = new TableRow();
        resultRow.setF(cellList);
        // For next
        fetch();
        return resultRow;
    }

    private void fetch() {
        try {
            nextLine = stream.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}