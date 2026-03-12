# DAG Scheduler (C++20)

Минимальный проект планировщика DAG.

## Сборка

```bash
cmake -S . -B build
cmake --build build
```

## Запуск

```bash
./build/dag_scheduler
./build/dag_scheduler docs/sample_graph.txt
```

## Тесты

```bash
ctest --test-dir build --output-on-failure
```

## Файлы

- `include/dag_scheduler.hpp` — реализация.
- `src/main.cpp` — демо.
- `tests/test_scheduler.cpp` — тесты.
- `docs/sample_graph.txt` — пример входного графа.
