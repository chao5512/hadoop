package mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by wangchao on 2018/9/27.
 */
public class GroupCompactor extends WritableComparator {
    protected GroupCompactor() {
        super(ScoreBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ScoreBean aBean = (ScoreBean) a;
        ScoreBean bBean = (ScoreBean) b;

        return aBean.getGroup().compareTo(bBean.getGroup());
    }
}
