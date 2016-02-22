package jp.gr.java_conf.bebigquery;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class TableRowsGcsStream implements Iterator<TableRow>, Iterable<TableRow> {
	private final Pattern CVS_ELEMENT_PATTERN = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");
	private final Logger logger = LoggerFactory.getLogger(TableRowsGcsStream.class);
	private Iterator<Storage.Objects.Get> gcsObjectIterator;
	private BufferedReader stream;
	private String nextLine;

	public TableRowsGcsStream(List<Storage.Objects.Get> getObjedtList) {
		gcsObjectIterator = getObjedtList.iterator();
	}

	@Override
	public Iterator<TableRow> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		// at first
		if (stream == null) {
			fetch();
		}
		//
		if (nextLine != null) {
			return true;
		} else {
			return gcsObjectIterator.hasNext();
		}
	}

	@Override
	public TableRow next() {
		if (nextLine == null) {
			throw new NoSuchElementException();
		}
		String result = nextLine;
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

	/**
	 * The purpose is updating nextLine
	 */
	private void fetch() {
		try {
			if (stream == null) {
				// at first
				downloadNextFileAndReadLine();
			} else {
				nextLine = stream.readLine();
				if (nextLine == null) {
					// if finished reading a current file.
					downloadNextFileAndReadLine();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void downloadNextFileAndReadLine() {
		try {
			if (gcsObjectIterator.hasNext()) {
				Storage.Objects.Get next = gcsObjectIterator.next();
				logger.info(next.toString());
				InputStream is = next.executeMediaAsInputStream();
				stream = new BufferedReader(new InputStreamReader(is));
				nextLine = stream.readLine();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}