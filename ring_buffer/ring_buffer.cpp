#include <atomic>
#include <iostream>
#include <thread>
#include <vector>



class RingBuffer {
  public:
  explicit RingBuffer(size_t size): data_(size, 0){}
    //The producer will push a value;
    inline bool push(uint32_t value){
      size_t const current_write_idx = head.load(std::memory_order_relaxed);
      size_t next_write_idx = current_write_idx+1;
      if (next_write_idx == data_.size()){
        //We wrap around
        next_write_idx = 0;
      }
      if (next_write_idx ==tail.load(std::memory_order_acquire)){
        return false;
      }
      data_[current_write_idx] = value;
      head.store(next_write_idx);
      return true;
    }
    
    //The consumer function
    inline bool pop(uint32_t& val){
      size_t current_read_index = tail.load(std::memory_order_acquire);
      if (head.load(std::memory_order_acquire) == current_read_index ){
        //We are on the write index
        return false;
      }
      val = data_[current_read_index];
      size_t next_read_index = current_read_index+1;
      if (next_read_index == data_.size()){
        next_read_index = 0;
      }
      tail.store(next_read_index, std::memory_order_relaxed);
      return true;      
    }

  private:
    std::vector<uint32_t> data_;
    //To avoid cacheline reads
    alignas(64) std::atomic<size_t> head{0};
    alignas(64) std::atomic<size_t> tail{0};
  
};

void producer(RingBuffer& rb,  size_t iterations){
  for (size_t i = 1; i < iterations ; ++i){
    while (!rb.push(static_cast<uint32_t>(i))){
      ;
    }
  }
}

void consumer(RingBuffer& rb,  size_t iterations){
  for (size_t i = 1; i<iterations; ++i){
    uint32_t val;
    while (!rb.pop(val)){
      ;
    }
  }
}

void benchmark(size_t iterations, size_t buffer_size){

  RingBuffer ring_buffer(buffer_size);

  auto start = std::chrono::steady_clock::now();
  
  std::thread producer_thread(producer, std::ref(ring_buffer), iterations);
  std::thread consumer_thread(consumer,std::ref(ring_buffer), iterations);
  producer_thread.join();
  consumer_thread.join();
  
  auto stop = std::chrono::steady_clock::now();
  
  auto time=  std::chrono::duration_cast<std::chrono::nanoseconds>(stop-start).count();
  auto iters_per_second = iterations * 1000000000 / time;
  std::cout << "Time: " << time << " ns \nIterations per second: " << iters_per_second << std::endl;
  
}

int main(int argc, char* argv[]){
  if (argc < 3 ){
    std::cerr << "Usage: ./spsc_test [buffer_size] [iterations]" << std::endl;
    return 1;
  }
  size_t buffer_size = std::stoi(argv[1]);
  size_t iterations = std::stoi(argv[2]);
  benchmark(iterations, buffer_size);
  return 0; 
}
