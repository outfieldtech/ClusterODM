const k8s = require('@kubernetes/client-node');
const AbstractASRProvider = require('../classes/AbstractASRProvider');
const logger = require('../logger');
const short = require('short-uuid');
const Node = require('../classes/Node');

module.exports = class KubernetesAsrProvider extends AbstractASRProvider {
    constructor(userConfig) {
        super({}, userConfig);
        this.k8sApi = new k8s.KubeConfig();
        this.k8sApi.loadFromDefault();
        this.coreV1Api = this.k8sApi.makeApiClient(k8s.CoreV1Api);
    }

    getDriverName() {
        return "kubernetes";
    }

    async getCreateArgs(imagesCount) {
        return {};
    }

    canHandle(imagesCount) {
        return true; // TODO
    }

    getMaxUploadTime(){
        return this.getConfig("maxUploadTime");
    }

    getDownloadsBaseUrl() {
        return "http://example.com/downloads"; // TODO
    }

    getMachinesLimit(){
        return this.getConfig("podLimit", -1);
    }

    getNamespace(){
        return this.getConfig("namespace", "default");
    }

    async initialize() {
        logger.info("Kubernetes ASR Provider initialized.");
    }

    generateHostname(imagesCount) {
        const randomString = Math.random().toString(36).substring(2, 10);
        return `nodeodm-${imagesCount}-${randomString}`.toLowerCase();
    }

    async createNode(req, imagesCount, token, hostname, status) {
        if (!this.canHandle(imagesCount)) throw new Error(`Cannot handle ${imagesCount} images.`);
    
        const podName = this.generateHostname(imagesCount);
        const nodeToken = short.generate();
        const podManifest = {
            metadata: {
                name: podName,
                labels: { app: 'clusterodm' }
            },
            spec: {
                containers: [{
                    name: 'nodeodm',
                    image: this.getConfig('dockerImage', 'opendronemap/nodeodm'),
                    resources: {
                        requests: { cpu: '2', memory: '4Gi' },
                        limits: { cpu: '2', memory: '4Gi' } // TODO: dynamically adjust based on nb images
                    }
                }]
            }
        };
    
        logger.info(`Creating pod with manifest: ${JSON.stringify(podManifest)}`);
    
        try {
            this.nodesPendingCreation++;
            await this.coreV1Api.createNamespacedPod(this.getNamespace(), podManifest);
            logger.info(`Pod ${podName} created successfully.`);
    
            // Wait for the pod to be ready and get its IP address
            let podIp = null;
            let retries = 0;
            const maxRetries = 30; // Maximum number of retries
            while (!podIp && retries < maxRetries) {
                const pod = await this.coreV1Api.readNamespacedPod(podName, this.getNamespace());
                podIp = pod.body.status.podIP;
                if (!podIp) {
                    logger.info(`Waiting for pod ${podName} to get an IP address...`);
                    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds
                    retries++;
                }
            }
    
            if (!podIp) {
                throw new Error(`Pod ${podName} failed to obtain an IP address after ${maxRetries} retries.`);
            }
    
            logger.info(`Pod ${podName} has IP address ${podIp}.`);
    
            // Create and return a new Node instance
            const newNode = new Node(podIp, this.getServicePort(), nodeToken);
            newNode.setDockerMachine(podName, this.getMaxRuntime(), this.getMaxUploadTime());
            return newNode;
        } catch (error) {
            logger.error(`Failed to create pod ${podName}:`, error);
            throw error;
        } finally {
            this.nodesPendingCreation--;
        }
    }

    async destroyNode(node) {
        if (node.isAutoSpawned()) {
            try {
                logger.debug(`Destroying pod ${node}`);
                await this.coreV1Api.deleteNamespacedPod(node.getDockerMachineName(), this.getNamespace());
                logger.info(`Pod ${node} destroyed successfully.`);
            } catch (error) {
                logger.error(`Failed to destroy pod ${node}: ${error.message}`);
            }
        } else {
            logger.warn(`Tried to call destroyNode on a non-autospawned node: ${node}`);
        }
    }

    async setupMachine(req, token, dm, nodeToken) {
        logger.info("setupMachine called, but no setup is required for Kubernetes.");
    }

    getNodesPendingCreation() {
        return this.nodesPendingCreation;
    }
}
