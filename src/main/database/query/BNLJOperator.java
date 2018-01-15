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

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator extends JoinIterator {
    // add any member variables here
    private int blockSize = numBuffers - 2;
    private BacktrackingIterator<Record> leftBlockIterator;
    private Iterator<Page> leftPageIterator;
    private BacktrackingIterator<Page> rightPageIterator;
    private BacktrackingIterator<Record> rightRecordIterator;
    private Record leftRecord;
    private Record nextRecord;
    private boolean rightIterUsed;
    private Page currentRightPage;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      leftPageIterator = BNLJOperator.this.getPageIterator(getLeftTableName());
      rightPageIterator = null;
      leftBlockIterator = null;
      rightRecordIterator = null;
      leftPageIterator.next(); // Throw away header page.
      leftRecord = null;
      nextRecord = null;



      //throw new UnsupportedOperationException("hw3: TODO");
    }

    public boolean hasNext() {
      //throw new UnsupportedOperationException("hw3: TODO");
      if (nextRecord != null) {
        return true;
      }

      while (true) {
        if (this.leftRecord == null) {

          if (this.rightPageIterator != null && this.rightPageIterator.hasNext()) { // If right page ended
            try {
              this.leftBlockIterator.reset();
              this.leftRecord = this.leftBlockIterator.next();
              currentRightPage = rightPageIterator.next();
              rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{currentRightPage});
              rightIterUsed = false;
            } catch (DatabaseException e) {
              return false;
            } catch (NoSuchElementException b) {
              return false;
            }
          } else if (leftPageIterator.hasNext()){  // If a new left block is needed.
            try {
              leftBlockIterator = BNLJOperator.this.getBlockIterator(getLeftTableName(), leftPageIterator, blockSize);
              leftRecord = leftBlockIterator.next();
              leftBlockIterator.mark(); // Mark that block start

              rightPageIterator = BNLJOperator.this.getPageIterator(getRightTableName());
              rightPageIterator.next(); // Throw away header
              currentRightPage = rightPageIterator.next();
              rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), new Page[] {currentRightPage});
              rightIterUsed = false;
            } catch (DatabaseException e) {
              return false;
            } catch (NoSuchElementException b) {
              return false;
            }
          } else {
            return false;
          }

        }

        while (this.rightRecordIterator.hasNext()) {  // Go through all records for page on the right.
          Record rightRecord = this.rightRecordIterator.next();
          if (!rightIterUsed) {
            rightRecordIterator.mark();
            rightIterUsed = true;
          }
          DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            return true;
          }
        }

        while (this.leftBlockIterator.hasNext()) { // Move a record on the left block.
          leftRecord = leftBlockIterator.next();
          this.rightRecordIterator.reset();
          return hasNext();
        }
        this.leftRecord = null; // Moving to a new block on the left.
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
