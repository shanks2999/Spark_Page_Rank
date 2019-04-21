import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class XmlInputFormat extends TextInputFormat {
//    public static final String START_TAG_KEY = "<article|<inproceedings|<proceedings|<book|<incollection|<phdthesis|<mastersthesis|<www|<person|<data";
//    public static final String END_TAG_KEY = "</article>|</inproceedings>|</proceedings>|</book>|</incollection>|</phdthesis>|</mastersthesis>|</www>|</person>|</data>";
//    public static final String START_TAG_KEY = "<inproceedings|<article";
//    public static final String END_TAG_KEY = "</inproceedings>|</article>";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }

    public static class XmlRecordReader extends
            RecordReader<LongWritable, Text> {
        private ArrayList<byte[]> startTag = new ArrayList<byte[]>();
        private ArrayList<byte[]> endTag = new ArrayList<byte[]>();
        private long start;
        private long end;
        private int currentTag = 0;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac)
                throws IOException, InterruptedException {
//            article|inproceedings|proceedings|book|incollection|
//                    phdthesis|mastersthesis|www|person|data
            FileSplit fileSplit = (FileSplit) is;
//            String START_TAG_KEY = "<inproceedings|<article|<proceedings|<book|<incollection|<phdthesis|<mastersthesis|<www|<person|<data";
//            String END_TAG_KEY = "</inproceedings>|</article>|</proceedings>|</book>|</incollection>|</phdthesis>|</mastersthesis>|</www>|</person>|</data>";
            String START_TAG_KEY = "<inproceedings|<article";
            String END_TAG_KEY = "</inproceedings>|</article>";
            String arrStartTag[] = START_TAG_KEY.split(Pattern.quote("|"));
            String arrEndTag[] = END_TAG_KEY.split(Pattern.quote("|"));

            for(int i=0;i<arrStartTag.length;i++) {
                startTag.add(arrStartTag[i].getBytes("utf-8"));
                endTag.add(arrEndTag[i].getBytes("utf-8"));
            }



            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
//            boolean foundIt = false;
//            long currentPos = fsin.getPos();
//            long pos = end;
//            int tagNo = 0;
            if (fsin.getPos() < end) {
//                fsin.mark( toIntExact(fsin.getPos()));
//                for(int t=0;t<startTag.size();t++) {
//                    if(readUntilMatch(startTag.get(t), false)) {
//                        long temp = fsin.getPos();
//                        if(pos>temp){
//                            pos = temp;
//                            tagNo = t;
//
//                            long abc = fsin.getPos();
//                            temp = fsin.getPos();
////                            fsin.mark();
//                        }
//                    }
//                    fsin.seek(currentPos);
//                }
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag.get(currentTag));
                        ArrayList<byte[]> gg = new ArrayList<byte[]>();
                        gg.add(endTag.get(currentTag));
                        if (readUntilMatch(gg, true)) {

//                            String temp = new String(buffer.getData());
////                            String gg = StringEscapeUtils.unescapeXml(temp);
//////                            String hh  = StringEscapeUtils.unescapeHtml(temp);
//                            String decodedXML= StringEscapeUtils.unescapeHtml4(temp);
////                            String a = Normalizer.normalize(decodedXML, Normalizer.Form.NFD);
//                            String b = StringUtils.stripAccents(decodedXML).trim();
//                            byte[] zz = b.getBytes("utf-8");
//                            value.set(zz, 0, buffer.getLength());
//                            value.set(buffer.getData(), 0, buffer.getLength());
                            byte utf[] = new byte[buffer.getLength()];
                            for (int i = 0; i < buffer.getLength(); i++) {
                                utf[i] = buffer.getData()[i];
                            }
                            String s = new String(utf);
                            value.set(s.trim());
                            key.set(fsin.getPos());


//                            System.out.println(buffer.getData().length);
//                            System.out.println(buffer.getLength());
//                            System.out.println(zz.length);
//                            foundIt = true;
//                            break;
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
//                }
                }
            }
//            if(foundIt)
//                return true;
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;

        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private boolean readUntilMatch(List<byte[]> match, boolean withinBlock)
                throws IOException {
            int[] counters=new int[match.size()];
            int count = 0;
            while (true) {
                int b = fsin.read();

                if (b == -1)
                    return false;

                if (withinBlock)
                    buffer.write(b);


                if(withinBlock) {
                    if (b == match.get(0)[count]) {
                        count++;
                        if (count >= match.get(0).length)
                            return true;
                    } else
                        count = 0;
                }
                else {
                    for(int i=0;i<match.size();i++) {
                        byte[] barr = match.get(i);
                        if (b == barr[counters[i]]) {
                            counters[i]++;
                            if (counters[i] >= barr.length) {
                                currentTag = i;
                                return true;
                            }
                        } else
                            counters[i] = 0;
                    }
                }
                for(int i=0;i<match.size();i++) {
                    if (!withinBlock && counters[i] == 0 && fsin.getPos() >= end)
                        return false;
                }
            }
        }
    }
}