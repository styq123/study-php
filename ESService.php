<?php
namespace app\service;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Client;
use Exception;
use InvalidArgumentException;
/**
 * Elasticsearch 服务类
 * 提供产品索引的CRUD操作和搜索功能
 */
class ESService{

        protected   $client;
        protected   $index;
        protected    $config;
        
        /**
         * 构造函数
         * @param array $config Elasticsearch配置
         */
        public function __construct($config = [])
        {
            $this->config = array_merge($this->getDefaultConfig(),$config);
            $this->index  =  $this->config['index'];
            $this->initializeClient();

        }
        /**
         * 获取默认配置
         */
        private function getDefaultConfig(){
                //  elasticsearch_username
            return [
                 'hosts'=>[env('ELASTICSEARCH_HOST'),"https://127.0.0.1:9200"],
                  'username'=>env('ELASTICSEARCH_USERNAME','elastic'),
                  'password'=>env('ELASTICSEARCH_PASSWORD',''),
                  'index'=>env('ELASTICSEARCH_INDEX','products'),
                  'ssl_verification'=>env('ELASTICSEARCH_SSL_VERIFICATION',FALSE),
                  'timeout'=>30,
                  'retries'=>3,

            ];
        }
        /**
         * 初始化 Elasticsearch 客户端
         */
        private function initializeClient(){

            try{
                $clientBuilder = ClientBuilder::create()
                ->setHosts($this->config['hosts'])
                ->setSSLVerification($this->config['ssl_verification']);
                if(!empty($this->config['username'])  &&  !empty($this->config['password']) ){
                    $clientBuilder->setBasicAuthentication(
                        $this->config['username'],
                        $this->config['password']
                    );
                }
                $this->client = $clientBuilder->build();
            }catch(Exception $e){
                    throw new Exception('Failed to initialize Elasticsearch client:'.$e->getMessage());
            }


        }
        /**
         * 添加文档到索引
         */
        public function add ($data,$id){

            if(empty($data)){
                throw new InvalidArgumentException("Docoument data cannot be empty");
            }
            $params = [
                'index'=>$this->index,
                'body'=>$data,
            ];
            if($id !== null){
                $params['id'] = $id;
            }
            try{
                return $this->client->index($params);
            }catch(Exception $e){
                throw new Exception ("Failed to add document:".$e->getMessage());
            }

        }
        /**
         * 根据ID获取文档
         */
        public function get($id){
            if(empty($id)){
                throw  new InvalidArgumentException("Document ID cannot be empety");
            }
            try{
                return $this->client->get([
                    'index'=>$this->index,
                    'id'=>$id,
                ]);

            }catch(Exception $e){
                throw new Exception("Failed to get document: " . $e->getMessage());
       
            }

        }
    /**
     * 根据ID删除文档
     * 
  */
    public function delete( $id) 
    {
        if (empty($id)) {
            throw new InvalidArgumentException("Document ID cannot be empty");
        }

        try {
            return $this->client->delete([
                'index' => $this->index,
                'id' => $id
            ]);
        } catch (Exception $e) {
            throw new Exception("Failed to delete document: " . $e->getMessage());
        }
    }
        /**
     * 更新文档
     * 
 
     */
    public function update(  $id,  $data) 
    {
        if (empty($id)) {
            throw new InvalidArgumentException("Document ID cannot be empty");
        }

        if (empty($data)) {
            throw new InvalidArgumentException("Update data cannot be empty");
        }

        try {
            return $this->client->update([
                'index' => $this->index,
                'id' => $id,
                'body' => [
                    'doc' => $data
                ]
            ]);
        } catch (Exception $e) {
            throw new Exception("Failed to update document: " . $e->getMessage());
        }
    }
    public function search($searchParams,$from = 0,$size = 10){
        if($from < 0 || $size <= 0){
            throw new InvalidArgumentException("Invalid pagination parameters");
        }
        
        $query = $this->buildSearchQuery($searchParams);
        $params = [
            'index'=>$this->index,
            'from'=>$from,
            'size'=>$size,
            'body'=>[
                'query'=>$query,
            ]
        ];
        try{
            return $this->client->search($params);
        }catch(Exception $e){
            throw new Exception ('Failed to search documents' . $e->getMessage());
        }

    }
    private function buildSearchQuery($searchParams){
            
        $shouldQueries = [];

        if(!empty($searchParams['goods_name'])){
            $shouldQueries[] = [
                'match'=>[
                    'goods_name'=>[
                        'query'=>$searchParams['goods_name'],
                        'boost'=>2.0,//提高商品名称的权重,
                        'fuzziness'=>'AUTO',//启用模糊搜索
                    ]
                ],
            ];
        }
        if(!empty($searchParams['min_price']) || ! empty($searchParams['max_price'])){
            
            $priceRange = [];
            if(!empty($searchParams['min_price'])){
                $priceRange['gte'] = $searchParams['min_price'];
            }
            if(!empty($searchParams['max_price'])){
                $priceRange['lte'] = $searchParams['max_price'];
            }
             if(!empty($priceRange)){
                $shouldQueries[] = [
                    'range'=>[
                        'price'=>$priceRange,
                    ]
                    ];
             }
     

        }
        if(!empty($searchParams['category_name'])){
            $shouldQueries[] = [
                'term'=>[
                    'category_name.keyword'=>$searchParams['category_name']
                ]
                ];
         }
         if(empty($shouldQueries)){

            return ['match_all'=>new \stdClass()];
         }
         return   [
            'bool'=>[
                'should'=>$shouldQueries,
                'minimum_should_match'=>1
            ],
        ];
    }
    //批量添加文档
    public function buldAdd($documents){

        if(empty($documents)){
            throw new InvalidArgumentException("Documents array cannot be empty");

        }
        $params = ['body'=>[]];

        foreach($documents as $doc){
            $params['body'][] = [
                'index'=>[
                    '_index'=>$this->index,
                    '_id'=>$doc['id'] ?? null
                ]
            ];
            $params['body'][] = $doc['data'];
        }

        try{
        return $this->client->bulk($params);
        }catch(Exception $e){
            throw new Exception("Failed to bulk add documents: " . $e->getMessage());
        }


    }
    //检查索引是否存在
    public function indexExists(){
        try {
         return $this->client->indices()->exists(['index'=>$this->index]);
        } catch (Exception $e) {
            return false;
        }
    }
    //创建索引
    public function createIndex($settings , $mappings ){
        $params = ['index'=>$this->index];
        if(!empty($settings)){
            $params['body']['settings'] = $settings;
        }
        if(!empty($mappings)){
            $params['body']['mappings'] = $mappings;
        }
        try {
            return $this->client->indices()->create($params);
        } catch (Exception $e) {
            throw new Exception("Failed to create index: " . $e->getMessage());
        }
    }
    /**
     * 删除索引
     * 
     * @return array
     * @throws Exception
     */
    public function deleteIndex(): array
    {
        try {
            return $this->client->indices()->delete(['index' => $this->index]);
        } catch (Exception $e) {
            throw new Exception("Failed to delete index: " . $e->getMessage());
        }
    }
        /**
     * 获取索引统计信息
     * 
     * @return array
     * @throws Exception
     */
    public function getIndexStats(): array
    {
        try {
            return $this->client->indices()->stats(['index' => $this->index]);
        } catch (Exception $e) {
            throw new Exception("Failed to get index stats: " . $e->getMessage());
        }
    }
        /**
     * 设置索引名称
     * 
     * @param string $index 索引名称
     * @return self
     */
    public function setIndex(string $index): self
    {
        $this->index = $index;
        return $this;
    }

    /**
     * 获取当前索引名称
     * 
     * @return string
     */
    public function getIndex(): string
    {
        return $this->index;
    }

    /**
     * 获取客户端实例
     * 
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }
}