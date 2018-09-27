package mr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wangchao on 2018/9/26.
 */
public class ScoreBean implements WritableComparable<ScoreBean> {
    private Integer group;
    private String name;
    private Double score;
    public ScoreBean(){
    }
    public ScoreBean(Integer group, String name, Double score) {
        this.group = group;
        this.name = name;
        this.score = score;
    }
    //在map阶段，按班级分组，对成绩排序
    public int compareTo(ScoreBean o) {
        int cmp = this.group.compareTo(o.getGroup());
        if(cmp == 0){
            cmp = -this.score.compareTo(o.getScore());
        }
        return cmp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(group);
        dataOutput.writeUTF(name);
        dataOutput.writeDouble(score);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.group = dataInput.readInt();
        this.name = dataInput.readUTF();
        this.score = dataInput.readDouble();
    }

    public Integer getGroup() {
        return group;
    }

    public void setGroup(Integer group) {
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return group + "\t" + name + "\t" + score;
    }
}
