package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {


  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator extends JoinIterator {
    // add any member variables here
    private BacktrackingIterator<Page> leftPageIterator;
    private Iterator<Page> rightPageIterator;
    private BacktrackingIterator<Record> leftRecordIterator;
    private BacktrackingIterator<Record> rightRecordIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Page currentLeftPage;
    private Page currentRightPage;
    private boolean rightIterUsed;


    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      //throw new UnsupportedOperationException("hw3: TODO");
      leftPageIterator = PNLJOperator.this.getPageIterator(getLeftTableName());
      leftPageIterator.next(); // Throw away header
      rightPageIterator = null;
      leftRecordIterator = null;
      rightRecordIterator = null;
      leftRecord = null;
      nextRecord = null;
    }

    public boolean hasNext() {
      //throw new UnsupportedOperationException("hw3: TODO");
      if(nextRecord != null) {
        return true;
      }
      while (true) {
        if (this.leftRecord == null) {
          if (this.rightPageIterator != null && this.rightPageIterator.hasNext()) { // Records in the left page available
            this.leftRecordIterator.reset();
            this.leftRecord = this.leftRecordIterator.next();
            try {
              currentRightPage = rightPageIterator.next();
              rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{currentRightPage});
              rightIterUsed = false;

            } catch (DatabaseException e) {
              return false;
            } catch (NoSuchElementException b) {
              return false;
            }
            // Pages are still available
          } else if (leftPageIterator.hasNext()) { // leftRecordIter is empty, but still have pages
            try {
              currentLeftPage = leftPageIterator.next();  // Get next left page and iterator from it
              leftRecordIterator = PNLJOperator.this.getBlockIterator(getLeftTableName(), new Page[]{currentLeftPage});
              leftRecord = leftRecordIterator.next(); // Get next record and mark it
              leftRecordIterator.mark(); // Beginning of page

              rightPageIterator = PNLJOperator.this.getPageIterator(getRightTableName());
              rightPageIterator.next(); // Throw away header
              currentRightPage = rightPageIterator.next();
              rightRecordIterator = PNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{currentRightPage});
              rightIterUsed = false;
            } catch (DatabaseException e) { // No pages to go through on the right.
              return false;
            } catch (NoSuchElementException b) {
              return false;
            }
          } else {
            return false;
          }
        }

        while (this.rightRecordIterator.hasNext()) {
          // loop through it and join
          Record rightRecord = this.rightRecordIterator.next();
          if (!rightIterUsed) {
            rightRecordIterator.mark();
            rightIterUsed = true;
          }
          DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            return true;
          }
        }

        while (this.leftRecordIterator.hasNext()) {
          leftRecord = leftRecordIterator.next();
          this.rightRecordIterator.reset();
          return hasNext();
        }
        this.leftRecord = null;
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      //throw new UnsupportedOperationException("hw3: TODO");
      if (this.hasNext()) {
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
