#ifndef MULTIQUEUEPROCESSOR_H
#define MULTIQUEUEPROCESSOR_H

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <sys/time.h>
#include <thread>
#include <queue>

#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>


std::string get_time()
{
  char buffer[30];
  struct timeval tv;

  time_t curtime;

  gettimeofday(&tv, NULL);
  curtime=tv.tv_sec;

  strftime(buffer,30,"%T.",localtime(&curtime));
  char res[30];
  memset(res, 0, sizeof(res)/sizeof(char));
  sprintf(res,"%s%ld",buffer,tv.tv_usec);
  return std::string(res);
}

const unsigned int minimal_worker_count = 1;

template<typename QueueKey, typename Value>
struct IConsumer
{
  IConsumer():id_(boost::uuids::to_string((boost::uuids::random_generator()()))){}

  virtual void Consume(QueueKey id, const Value &value)
  {
    std::cout <<id_<<" "<<value<< " "<<get_time()<<std::endl;
  }
  virtual ~IConsumer()
  {

  }
  std::string id_;
};

template<typename QueueKey, typename Value>
class MultiQueueProcessor
{
  struct Triple
  {
    Value value_;
    QueueKey key_;
    std::shared_ptr<IConsumer<QueueKey, Value>> consumer_;
  };
public:
  MultiQueueProcessor() :
  running_{ true },
  sleep_time_process_ms_(100),
  sleep_time_worker_ms_(100),
  max_capacity_(50),
  worker_count_(std::thread::hardware_concurrency()-1),
  queue_thread_(worker_count_ < minimal_worker_count ? minimal_worker_count : worker_count_),
  wmtx_(worker_count_ < minimal_worker_count ? minimal_worker_count : worker_count_)
  {
    if (worker_count_ < minimal_worker_count)
    {
      worker_count_ = minimal_worker_count;
    }
    for (unsigned int i = 0; i < worker_count_; ++i)
    {
     working_threads_.create_thread(boost::bind(&MultiQueueProcessor::Worker, this, i));
    }
    working_threads_.create_thread(boost::bind(&MultiQueueProcessor::Process, this));
  }

  ~MultiQueueProcessor()
  {
    StopProcessing();
    working_threads_.join_all();
  }

  void StopProcessing()
  {
    running_ = false;
  }

  void Subscribe(QueueKey id, std::shared_ptr<IConsumer<QueueKey, Value>> consumer)
  {
    auto iter = consumers_.find(id);
    if (iter == consumers_.end())
    {
      std::unique_lock<std::recursive_mutex> lock{mtx_};
      consumers_.insert(std::pair<QueueKey, std::shared_ptr<IConsumer<QueueKey, Value> > >(id, std::move(consumer)));
      queues_.emplace(std::make_pair(id, std::queue<Value>()));
    }
  }

  void Unsubscribe(QueueKey id)
  {
    auto iter = consumers_.find(id);
    if (iter != consumers_.end())
    {
      std::unique_lock<std::recursive_mutex> lock{mtx_};
      consumers_.erase(id);
      auto t = consumers_.find(id);
    }
  }

  bool Enqueue(QueueKey id, Value value)
  {
    bool ret = false;
    try
    {
      std::unique_lock<std::recursive_mutex> lock{mtx_};
      auto v = queues_.find(id);
      if (v != queues_.end() && v->second.size() < max_capacity_)
      {
        v->second.emplace(value);
        ret = true;
      }
    }
    catch(std::bad_alloc& ba)
    {
      std::cout << "bad_alloc in Enqueue : " << ba.what() << std::endl;
    }
    return ret;
  }
protected:
  void Worker(unsigned int i)
  {
    while (running_)
    {
      try 
      {
        std::unique_lock<std::recursive_mutex> lock{wmtx_[i]};
        if (queue_thread_[i].size() >= max_capacity_)
        {
          while (queue_thread_[i].size() > max_capacity_/2)
          {
            auto tmp = queue_thread_[i].front();
            tmp.consumer_->Consume(tmp.key_, tmp.value_);
            queue_thread_[i].pop();
          }
        }
        else
        {
          if (!queue_thread_[i].empty())
          {
            auto tmp = queue_thread_[i].front();
            queue_thread_[i].pop();
            tmp.consumer_->Consume(tmp.key_, tmp.value_); 
          }
        }
      }
      catch(std::bad_alloc& ba)
      {
        std::cout << "bad_alloc in worker "<<i << " : " << ba.what()<< std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time_worker_ms_));
    }
  }

  void Process()
  {
    while (running_)
    {
      try
      {
        unsigned int k = 0, tmpind = 0;
        for (auto iter = queues_.begin(); iter != queues_.end(); ++iter, ++k)
        {
          if (iter->second.empty())
              continue;
          if (iter->second.size() >= max_capacity_)
          {
            if (consumers_.find(iter->first) == consumers_.end())
            {
              continue;
            }
            std::unique_lock<std::recursive_mutex> lock{mtx_};
            while (iter->second.size() > max_capacity_/2)
            {
              Triple elem = Triple{iter->second.front(), iter->first, consumers_[iter->first]};
              iter->second.pop();
              tmpind = k++;
              std::unique_lock<std::recursive_mutex> stdlock{ wmtx_[tmpind % (worker_count_)] };
              queue_thread_[tmpind % (worker_count_)].emplace(elem);
            }
          }
          else
          {
            if (consumers_.find(iter->first) == consumers_.end())
            {
              continue;
            }
            std::unique_lock<std::recursive_mutex> lock{mtx_};
            Triple elem = Triple{iter->second.front(), iter->first, consumers_[iter->first]};
            iter->second.pop();
            tmpind = k++;
            std::unique_lock<std::recursive_mutex> stdlock{ wmtx_[tmpind % (worker_count_)] };
            queue_thread_[tmpind % (worker_count_)].emplace(elem);
          }
        }
      }
      catch(std::bad_alloc& ba)
      {
        std::cout << "Bad alloc in process : " << ba.what() << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time_process_ms_));
    }
  }

protected:
  unsigned int worker_count_;
  std::map<QueueKey, std::shared_ptr<IConsumer<QueueKey, Value>>> consumers_;
  std::map<QueueKey, std::queue<Value>> queues_;

  boost::thread_group working_threads_;

  std::vector<std::queue<Triple>> queue_thread_;
  mutable std::vector<std::recursive_mutex> wmtx_;

  std::atomic<bool> running_;
  mutable std::recursive_mutex mtx_;
  int sleep_time_process_ms_;
  int sleep_time_worker_ms_; 
  int max_capacity_;
};

#endif

