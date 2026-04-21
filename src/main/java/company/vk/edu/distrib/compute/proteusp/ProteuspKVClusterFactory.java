package company.vk.edu.distrib.compute.proteusp;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.proteusp.dao.ProteusPFSDao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("java:S1125")
public class ProteuspKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {

        return doCreate(ports, new ConsistentHashing());
    }

    private KVCluster doCreate(List<Integer> ports, ShardingAlgorithm shardingAlgorithm) {
        Map<String, ProteuspKVNode> nodes = new ConcurrentHashMap<>();
        List<String> endpoints = new ArrayList<>();

        for (Integer port : ports) {
            String endpoint = "http://localhost:" + port;
            endpoints.add(endpoint);

            ProteuspKVNode node = new ProteuspKVNode(
                    port,
                    new ProteusPFSDao(ProteuspKVNode.class),
                    endpoints,
                    shardingAlgorithm,
                    endpoint
            );
            nodes.put(endpoint, node);
            node.start();
        }
        return new ProteuspKVCluster(nodes);
    }
}
