package io.quarkus.kafka.client.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class KafkaInfoSupplier implements Supplier<Collection<KafkaInfo>> {

    @Override
    public List<KafkaInfo> get() {
        List<KafkaInfo> infoList = new ArrayList<>(getKafkaInfos());

        //TODO: get the info from Kafka runtime
        return infoList;
    }
    
    public static List<KafkaInfo> getKafkaInfos(){
        List<KafkaInfo> infoList = new ArrayList<>();
        KafkaInfo ki = new KafkaInfo();
        infoList.add(ki); 
        return infoList;
    }
}
