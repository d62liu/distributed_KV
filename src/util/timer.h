  #pragma once
  #include <functional>                                                                                                       
  #include <thread>
  #include <chrono>

  class Timer {
  public:
      explicit Timer(std::function<void()> callback);
                                                                                                                              
  private:
      void run();                                                                                                             
                  
      std::function<void()> callback;
      std::thread thread;
      std::chrono::milliseconds duration{0};
  };