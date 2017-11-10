from .cluster_task import ClusterTask
from .cluster_manager import ClusterManager
from .sge_cluster_manager import SgeClusterManager
from .slurm_cluster_manager import SlurmClusterManager

__all__ = ['ClusterTask', 'ClusterManager', 'SgeClusterManager',
           'SlurmClusterManager', 'CeleryClusterManager']


class CeleryClusterManager(ClusterManager):
    def enqueue_inner(self, task):
        raise NotImplementedError
