import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinationCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        protected void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //System.out.println("------------Into map!-------------");
            //System.out.println("Value: "+value.toString());
            String[] data = value.toString().split("\\|",-1);
            System.out.println("Data[0]: "+data[0]);
            DataProcess dp = new DataProcess(data);
            dp.combine(context);
        }

        //os_dict = {'24':11, '25':12, '26':13, '27':14, '11':31, '12':32, '13':33, '14':34}
        private final static String[] os_k = {"24", "25", "26", "27", "11", "12", "13", "14"};
        private final static String[] os_v = {"11", "12", "13", "14", "31", "32", "33", "34"};

        protected class DataProcess {
            private String adx="";
            private String device_device_type="";
            private String networkConnection_connection_type="";
            private String device_os_version="";
            private String networkConnection_carrier_id="";
            private String app_category_id="";
            private String location_id="";
            private String device_model="";
            private String app_id="";
            //private final int RED = 0x1C0;
            private final int GREY = 0x38;
            private ArrayList<Integer> flags;
            private ArrayList<String> combs;
            /*
                # adx                                   :int,          --U 0
                # device_device_type                    :int,          --u 1
                # detworkConnection_connection_type     :int,          --U 2
                # device_os                             :int,          --U 3
                # device_os_version                     :int,          --U 3
                # detworkConnection_carrier_id          :int,          --U 4
                # app_category_id                       :int,          --U 5
                x# location_country_id                  :int,          --U 6
                x# location_region_id                   :int,          --U 6
                x# location_city_id                     :int,          --U 6
                # location_geo_criteria_id              :int,          --U 6
                # device_brand                          :chararray,    --u 7
                # device_model                          :chararray,    --u 7
                # app_limei_app_id                      :int,          --U 8
             */
            public DataProcess(String[] record) {
                System.out.println("------------Data process record length: "+record.length);
                this.flags = new ArrayList<Integer>();
                this.combs = new ArrayList<String>();
                this.flags.add(0);
                this.combs.add("");
                //adx
                this.adx = record[0];
                //device_device_type
                this.device_device_type = record[18];
                //networkConnection_connection_type
                this.networkConnection_connection_type = record[22];
                //networkConnection_carrier_id
                this.networkConnection_carrier_id = record[23];
                //app_category_id
                this.app_category_id = record[40];
                //device_os device_os_version
                if (!record[15].equals("")) {
                    String k = record[14] + record[15].substring(0, 1);
                    int i=0;
                    for(;i<os_k.length;++i){
                        if(os_k[i].equals(k)){
                            this.device_os_version = os_v[i];
                            break;
                        }
                    }
                    if(i>=os_k.length){
                        this.device_os_version = "";
                    }
                } else {
                    this.device_os_version = record[14];
                }
                //location_country_id location_region_id location_city_id
                /*
                System.out.println("adx:"+this.adx+"\tcountry:"+record[28]+"\tregion:"+record[29]+"\tcity:"+record[30]);
                if(!record[30].equals("")){
                    this.location_id=record[30];
                }else{
                    if(!record[29].equals("")){
                        this.location_id=record[29];
                    }else{
                        this.location_id=record[28];
                    }
                }*/
                this.location_id = record[43];
                //device_brand device_model
                if(!record[17].equals("")){
                    this.device_model=record[17];
                }else{
                    this.device_model=record[16];
                }
                //app_limei_app_id
                this.app_id=record[45];
                //System.out.println("------------DATA PROCESS SETED!-------------");
            }
            public void combine(Context context){
                writeToCT(context,this.adx,0,false,false);
                writeToCT(context,this.device_device_type,1,false,false);
                writeToCT(context,this.networkConnection_connection_type,2,false,false);
                writeToCT(context,this.device_os_version,3,true,false);
                writeToCT(context,this.networkConnection_carrier_id,4,true,false);
                writeToCT(context,this.app_category_id,5,true,false);
                writeToCT(context,this.location_id,6,false,true);
                writeToCT(context,this.device_model,7,false,true);
                writeToCT(context,this.app_id,8,false,true);
            }
            private boolean writeToCT(Context co, String cur,int level, boolean isGrey, boolean isRed){
                if(cur.equals("")){
                    return false;
                }

                int comb_len=this.combs.size();
                for(int i=0;i<comb_len;++i){
                    int pf = (int)this.flags.get(i);
                    if(isGrey && (digitsSeted(pf&GREY) > 1)){
                        continue;
                    }
                    if(isRed && (digitsSeted(pf) > 1)){
                        continue;
                    }
                    if(level == 5){
                        String [] wa = cur.split(",");
                        for(int wi=0;wi<wa.length;++wi){
                            String w = (String)this.combs.get(i)+"."+wa[wi];
                            int f = pf | 1 << level;
                            this.combs.add(w);
                            this.flags.add(f);
                            try {
                                word.set(""+f+w);
                                co.write(word, one);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }else{
                        String w = (String)this.combs.get(i)+"."+cur;
                        int f = pf | 1 << level;
                        this.combs.add(w);
                        this.flags.add(f);
                        try {
                            word.set(""+f+w);
                            co.write(word, one);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                }
                return true;
            }
            private int digitsSeted(int x){
                int count = 0;
                for(int i=0;i<9;++i){
                    if((x & (1<<i)) !=0)
                        count++;
                }
                return count;
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "combination count");
        job.setJarByClass(CombinationCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}