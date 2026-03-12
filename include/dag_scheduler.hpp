#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dag {

struct Task {
    int id{};
    std::function<void()> action;
};

class Dag {
public:
    void add_task(Task task) {
        if (!task.action) {
            throw std::invalid_argument("Для задачи не задана функция выполнения");
        }
        if (tasks_.count(task.id) > 0) {
            throw std::invalid_argument("Задача с таким id уже существует");
        }

        const int id = task.id;
        tasks_.emplace(id, std::move(task));
        adjacency_[id];
        indegree_.try_emplace(id, 0);
    }

    void add_dependency(int from, int to) {
        ensure_exists(from);
        ensure_exists(to);
        adjacency_[from].push_back(to);
        ++indegree_[to];
    }

    [[nodiscard]] std::vector<int> topological_sort() const {
        std::unordered_map<int, int> current_indegree = indegree_;
        std::queue<int> q;

        for (const auto& [id, _] : tasks_) {
            if (current_indegree[id] == 0) {
                q.push(id);
            }
        }

        std::vector<int> order;
        order.reserve(tasks_.size());

        while (!q.empty()) {
            int node = q.front();
            q.pop();
            order.push_back(node);

            auto it = adjacency_.find(node);
            if (it == adjacency_.end()) {
                continue;
            }

            for (int next : it->second) {
                if (--current_indegree[next] == 0) {
                    q.push(next);
                }
            }
        }

        if (order.size() != tasks_.size()) {
            throw std::runtime_error("Обнаружен цикл в графе зависимостей");
        }

        return order;
    }

    [[nodiscard]] const Task& task(int id) const {
        auto it = tasks_.find(id);
        if (it == tasks_.end()) {
            throw std::out_of_range("Задача не найдена");
        }
        return it->second;
    }

    [[nodiscard]] const std::vector<int>& dependents(int id) const {
        auto it = adjacency_.find(id);
        if (it == adjacency_.end()) {
            throw std::out_of_range("Задача не найдена");
        }
        return it->second;
    }

    [[nodiscard]] std::unordered_map<int, int> indegree_map() const {
        return indegree_;
    }

    [[nodiscard]] std::size_t size() const {
        return tasks_.size();
    }

private:
    void ensure_exists(int id) const {
        if (tasks_.count(id) == 0) {
            throw std::invalid_argument("Попытка создать зависимость для несуществующей задачи");
        }
    }

    std::unordered_map<int, Task> tasks_;
    std::unordered_map<int, std::vector<int>> adjacency_;
    std::unordered_map<int, int> indegree_;
};

class Scheduler {
public:
    explicit Scheduler(std::size_t thread_count)
        : thread_count_(thread_count == 0 ? 1 : thread_count) {}

    void execute(const Dag& dag) {
        if (dag.size() == 0) {
            return;
        }

        (void)dag.topological_sort();

        std::unordered_map<int, int> remaining = dag.indegree_map();
        std::queue<int> ready;
        for (const auto& [id, indegree] : remaining) {
            if (indegree == 0) {
                ready.push(id);
            }
        }

        std::mutex m;
        std::condition_variable cv;
        std::atomic<std::size_t> completed{0};
        bool stop = false;
        std::exception_ptr first_error;

        auto worker = [&]() {
            while (true) {
                int task_id = -1;
                {
                    std::unique_lock<std::mutex> lock(m);
                    cv.wait(lock, [&]() {
                        return stop || !ready.empty();
                    });

                    if (stop && ready.empty()) {
                        return;
                    }

                    if (ready.empty()) {
                        continue;
                    }

                    task_id = ready.front();
                    ready.pop();
                }

                try {
                    dag.task(task_id).action();
                } catch (...) {
                    std::lock_guard<std::mutex> lock(m);
                    if (!first_error) {
                        first_error = std::current_exception();
                    }
                    stop = true;
                    cv.notify_all();
                    return;
                }

                {
                    std::lock_guard<std::mutex> lock(m);
                    for (int next : dag.dependents(task_id)) {
                        int& in = remaining[next];
                        --in;
                        if (in == 0) {
                            ready.push(next);
                        }
                    }
                    const auto done = ++completed;
                    if (done == dag.size()) {
                        stop = true;
                    }
                }
                cv.notify_all();
            }
        };

        std::vector<std::thread> pool;
        pool.reserve(thread_count_);
        for (std::size_t i = 0; i < thread_count_; ++i) {
            pool.emplace_back(worker);
        }

        {
            std::lock_guard<std::mutex> lock(m);
            cv.notify_all();
        }

        for (auto& t : pool) {
            if (t.joinable()) {
                t.join();
            }
        }

        if (first_error) {
            std::rethrow_exception(first_error);
        }

        if (completed != dag.size()) {
            throw std::runtime_error("Планировщик завершился до выполнения всех задач");
        }
    }

private:
    std::size_t thread_count_;
};

}  // namespace dag
