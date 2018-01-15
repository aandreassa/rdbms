package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }


  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator extends JoinIterator {
    //add any member variables you need here
    BacktrackingIterator<Record> leftRecordIter;
    BacktrackingIterator<Record> rightRecordIter;
    SortOperator sortLeft;
    SortOperator sortRight;
    String leftTableSortedName;
    String rightTableSortedName;
    Record leftRecord;
    Record rightRecord;
    Record nextRecord;
    boolean leftMarked;
    boolean sMarked;
    boolean inner;
    boolean outer;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      sortLeft = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
      sortRight = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(), new RightRecordComparator());
      leftTableSortedName = sortLeft.sort();
      rightTableSortedName = sortRight.sort();
      sMarked = false;
      leftRecordIter = null;
      rightRecordIter = null;
      leftRecord = null;
      rightRecord = null;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      //throw new UnsupportedOperationException("hw3: TODO");
      if (nextRecord != null) {
        return true;
      }
      while (true) {
          try {                                             // Initialize
            if (leftRecordIter == null) {
              leftRecordIter = SortMergeOperator.this.getRecordIterator(leftTableSortedName);
              rightRecordIter = SortMergeOperator.this.getRecordIterator(rightTableSortedName);
              leftRecord = leftRecordIter.next();
              rightRecord = rightRecordIter.next();
            }
          } catch (DatabaseException e){
            return false;
          } catch (NoSuchElementException b) {
            return false;
          }
          if (leftRecord == null || rightRecord == null) { // If any of them ended, false
            return false;
          }

          DataBox leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

          if (leftJoinValue.compareTo(rightJoinValue) < 0) {  // r < s
            if (sMarked) {
              rightRecordIter.reset();
              rightRecord = (rightRecordIter.hasNext()) ? rightRecordIter.next() : null;
            }
            leftRecord = (leftRecordIter.hasNext()) ? leftRecordIter.next() : null;
          } else if (leftJoinValue.compareTo(rightJoinValue) > 0) { // r > s
            if (sMarked) {                                      // Mark becomes invalid
              sMarked = false;
            }
            rightRecord = (rightRecordIter.hasNext()) ? rightRecordIter.next() : null;

          } else {                                            // r == s
            if (!sMarked) {
              sMarked = true;
              rightRecordIter.mark();
            }
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            if (rightRecordIter.hasNext()) {             // Advance S. (inner r == s)
              rightRecord = rightRecordIter.next();
            } else if (leftRecordIter.hasNext()) {       // Advance R if S ended.
              rightRecordIter.reset();
              rightRecord = rightRecordIter.next();
              leftRecord = leftRecordIter.next();
            } else {                                       // If there is no mark for S, end.
              leftRecord = null;
            }
            return true;
          }
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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
