# Name                     , DisplayName                , Description
time                       , 时间                       ,

region                     , 区域                       ,
az                         , 可用区                     ,
host                       , 宿主机                     , 承载虚拟机的宿主机。
chost                      , 云服务器                   , 包括虚拟机、裸金属服务器。
vpc                        , VPC                        ,
subnet                     , 子网                       ,
router                     , 路由器                     ,
dhcpgw                     , DHCP 网关                  ,
lb                         , 负载均衡器                 ,
lb_listener                , 负载均衡监听器             ,
natgw                      , NAT 网关                   ,
redis                      , Redis                      ,
rds                        , RDS                        ,
pod_cluster                , K8s 容器集群               ,
pod_ns                     , K8s 命名空间               ,
pod_node                   , K8s 容器节点               ,
pod_ingress                , K8s Ingress                ,
pod_service                , K8s 容器服务               ,
pod_group_type             , K8s 工作负载类型           ,
pod_group                  , K8s 工作负载               , 例如 Deployment、StatefulSet、Daemonset 等。
pod                        , K8s 容器 POD               ,
service                    , 服务                       , 
resource_gl0_type          , 自动实例类型                , 已废弃，请使用 auto_instance_type。
resource_gl0               , 自动实例标签                , 已废弃，请使用 auto_instance。
resource_gl1_type          , 类型-工作负载优先          , 已废弃，请使用 auto_service_type。
resource_gl1               , 资源-工作负载优先          , 已废弃，请使用 auto_service。
resource_gl2_type          , 自动资源类型                , 已废弃，请使用 auto_service_type。
resource_gl2               , 自动资源标签                , 已废弃，请使用 auto_service。
auto_instance_type         , 自动实例类型                , `auto_instance`实例对应的类型。
auto_instance              , 自动实例标签                , IP 对应的实例，实例为IP时，auto_instance_id显示为子网ID。
auto_service_type          , 自动资源类型                , `auto_service`实例对应的类型。
auto_service               , 自动资源标签                , 在`auto_instance`基础上，将容器服务的 ClusterIP 与工作负载聚合为服务，实例为IP时，auto_service_id显示为子网ID。
host_ip                    , 宿主机                     , 宿主机的管理 IP。
host_hostname              , 宿主机                     , 宿主机的 Hostname。
chost_ip                   , 云服务器                   , 云服务器的主 IP。
chost_hostname             , 云服务器                   , 云服务器的 Hostname。
pod_node_ip                , K8s 容器节点               , 容器节点的主 IP。
pod_node_hostname          , K8s 容器节点               , 容器节点的 Hostname。

k8s.label                  , K8s Label                  ,
k8s.annotation             , K8s Annotation             ,
k8s.env                    , K8s Env                    ,
cloud.tag                  , Cloud Tag                  ,
os.app                     , OS APP                     ,

ip                         , IP 地址                    ,
is_ipv4                    , IPv4 标志                  ,

vtap                       , 采集器                     , 已废弃，请使用 agent。
agent                      , 采集器                     ,
