package redisinterface

import (
	"github.com/fzzy/radix/redis"
	"strings"
)

// todo panic on closed socket (i.e. crashed server)

const redisDividerChar string = ":"

func RedisDividerChar() string {
	return redisDividerChar
}

func SetupRedisConnection() *redis.Client {
	c, e := redis.Dial("tcp", ":6379")

	if e != nil {
		panic(e)
	}
	return c
}

func StartMulti(redisClient *redis.Client) {
	var reply *redis.Reply = redisClient.Cmd("MULTI")
	if reply.Err != nil {
		panic(reply.Err)
	}
}

func Exec(redisClient *redis.Client) {
	var reply *redis.Reply = redisClient.Cmd("EXEC")
	if reply.Err != nil {
		panic(reply.Err)
	}
}

func SetRedisValue(redisClient *redis.Client, key string, value string) {	
	var reply *redis.Reply = redisClient.Cmd("SET", key, value)

	if reply.Err != nil {
		panic(reply.Err);
	}
}

func PushLeftListInt(redisClient *redis.Client, key string, value int) error {
	var reply *redis.Reply = redisClient.Cmd("LPUSH", key, value)
	return reply.Err
}

func PushLeftListString(redisClient *redis.Client, key string, value string) error {
	var reply *redis.Reply = redisClient.Cmd("LPUSH", key, value)
	return reply.Err
}

func ListItems(redisClient *redis.Client, key string, offset int64, number int64) ([]string, error) {
	var reply *redis.Reply = redisClient.Cmd("LRANGE", key, offset, number)
	return reply.List()
}

func RemoveStringFromList(redisClient *redis.Client, key string, value string) error {
	var reply *redis.Reply = redisClient.Cmd("LREM", key, 1, value)
	return reply.Err
}

func AddSetMember(redisClient *redis.Client, key string, value string) error {
	var reply *redis.Reply = redisClient.Cmd("SADD", key, value)
	return reply.Err
}

func SetMembers(redisClient *redis.Client, key string) ([]string, error) {
	var reply *redis.Reply = redisClient.Cmd("SMEMBERS", key)
	return reply.List()
}

func GetRedisStringValue(redisClient *redis.Client, key string) (string, error) {	
	var reply *redis.Reply = redisClient.Cmd("GET", key)
	return reply.Str();
}

func GetRedisHashStringValue(redisClient *redis.Client, hashKey string, key string) (string, error) {
	var reply *redis.Reply = redisClient.Cmd("HGET", hashKey, key)
	return reply.Str();
}

func DeleteHashField(redisClient *redis.Client, hashKey string, key string) error {
	var reply *redis.Reply = redisClient.Cmd("HDEL", hashKey, key)
	return reply.Err
}

func UniqueIdForObjectType(redisClient *redis.Client, objectType string) (int64, error) {
	var s []string = []string{"uniqueid", redisDividerChar, objectType};
	incrementKey := strings.Join(s, "");
	reply := redisClient.Cmd("INCR", incrementKey);
	return reply.Int64()
}

func IncrementRedisValue(redisClient *redis.Client, key string) {
	var reply *redis.Reply = redisClient.Cmd("INCR", key)
	if reply.Err != nil {
		panic(reply.Err)
	}
}

func SetHashStringValue(redisClient *redis.Client, hashKey string, key string, value string) error {
	var reply *redis.Reply = redisClient.Cmd("HSET", hashKey, key, value)
	return reply.Err
}

func DeleteHashKeyValue(redisClient *redis.Client, hashKey string, key string) error {
	var reply *redis.Reply = redisClient.Cmd("HDEL", hashKey, key)
	return reply.Err
}
