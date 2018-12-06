// Copyright (c) 2018 PaddlePaddle Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// There are some bugs in the implementation of the two lock
// blocking queue(using 36 thread will meet bad_malloc bug).
// Now use the lock blocking queue for the time being.
template <typename T>
class BlockingQueue {
 public:
  explicit BlockingQueue(size_t capacity = 32)
      : capacity_(capacity), closed_(false) {
    size_.store(0);
  }

  bool Send(T& elem) {  // NOLINT
    int c = -1;
    {
      std::unique_lock<std::mutex> lock(send_mutex_);
      send_cv_.wait(lock, [&] { return size_.load() < capacity_ || closed_; });
      if (closed_) {
        VLOG(5)
            << "WARNING: Sending an element to a closed reader::BlokcingQueue.";
        return false;
      }
      queue_.push_back(elem);
      c = size_.load();
      size_.fetch_add(1);
      if (c + 1 < capacity_) {
        send_cv_.notify_one();
      }
    }

    if (c == 0) {
      std::unique_lock<std::mutex> lock(receive_mutex_);
      receive_cv_.notify_one();
    }
    return true;
  }

  bool Receive(T* elem) {
    int c = -1;
    {
      std::unique_lock<std::mutex> lock(receive_mutex_);
      receive_cv_.wait(lock, [&] { return size_.load() != 0 || closed_; });
      if (size_.load() != 0) {
        *elem = queue_.front();
        queue_.pop_front();
        c = size_.load();
        size_.fetch_sub(1);
        if (c > 1) {
          receive_cv_.notify_one();
        }
      } else {
        return false;
      }
    }
    if (c == capacity_) {
      std::unique_lock<std::mutex> lock(send_mutex_);
      send_cv_.notify_one();
    }
    return true;
  }

  void Close() {
    std::lock_guard<std::mutex> lock1(send_mutex_);
    std::lock_guard<std::mutex> lock2(receive_mutex_);
    closed_ = true;
    send_cv_.notify_all();
    receive_cv_.notify_all();
  }

 private:
  size_t capacity_;
  std::atomic_size_t size_;
  bool closed_;
  std::deque<T> queue_;

  mutable std::mutex send_mutex_;
  mutable std::mutex receive_mutex_;
  mutable std::condition_variable send_cv_;
  mutable std::condition_variable receive_cv_;
};
