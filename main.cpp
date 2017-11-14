#include <memory>
#include <ratio>
#include <string>
#include <thread>
#include <vector>

#include "MultiQueueProcessor.h"


std::vector<std::thread> producers;

std::atomic<bool> stopAllProducers{false};

const int ConsumerNumber = 5;
const int ProducerNumber = 10;

void produce(void* mqp, int ind)
{
  MultiQueueProcessor<int, std::string>* mqp1 = (MultiQueueProcessor<int, std::string>*)mqp;
  while (!stopAllProducers)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::string ct = get_time();
    std::cout<<"Enqueue "<<"queue#"<<ind<<" time "<<ct<<std::endl;
    mqp1->Enqueue(ind, ct);
  }
}

int main()
{
  MultiQueueProcessor<int, std::string> mqp;
  for (size_t i = 0; i < ProducerNumber; ++i)
  {
    producers.push_back(std::thread(produce, (void*)&mqp, i% ConsumerNumber));
  }

  std::vector<std::shared_ptr<IConsumer<int, std::string>>> tmp_consumers;
  for (size_t i = 0; i < ConsumerNumber; ++i)
  {
    std::shared_ptr<IConsumer<int, std::string>> tmp(new IConsumer<int, std::string>());
    mqp.Subscribe(i, tmp);
    tmp_consumers.push_back(std::move(tmp));
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));

  mqp.Unsubscribe(1);
  mqp.Unsubscribe(2);
  mqp.Unsubscribe(3);
  mqp.Unsubscribe(4);
  std::cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;

  std::this_thread::sleep_for(std::chrono::seconds(5));
  
  stopAllProducers = true;
  
  for (size_t i = 0; i < ProducerNumber; ++i)
  {
    producers[i].join();
    std::cout << "produccer " << i <<" stopped " <<get_time() << std::endl;
  }
  std::cout << "finish " << get_time() << std::endl;
  return 0;
}


