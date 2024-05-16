# DistributedEvent

## Общее назначение

`DistributedEvent` — это класс для управления распределённым событием с использованием Redis. Этот класс позволяет
разным процессам или сервисам, работающим в различных средах, синхронизировать свои действия по событию, используя
механизмы публикации и подписки Redis. Событие может быть установлено в состояние "сигнал", после чего ожидающие
процессы могут продолжить свою работу.

## Использование Redis

Класс `DistributedEvent` использует Redis для создания флага события и канала публикации/подписки. Ключ в
Redis (`_name`) используется для отслеживания состояния события (установлено или нет), а через канал (`_channel_name`)
происходит оповещение подписчиков о его изменении.

## Методы класса

### `__init__(self, redis: 'Redis[Any]', name: str, consumer_timeout: float = 5)`

Инициализирует экземпляр распределённого события.

- `redis`: клиент Redis, который будет использоваться для управления событием.
- `name`: имя события, используемое в ключах Redis.
- `consumer_timeout`: время ожидания сообщения в подписке, в секундах.

### `async is_set(self) -> bool`

Проверяет, установлено ли событие. Возвращает `True`, если флаг внутреннего состояния равен `1`.

### `async set(self) -> None`

Устанавливает внутренний флаг события в состояние `True` и оповещает всех ожидающих корутин.

### `async clear(self) -> None`

Сбрасывает внутренний флаг события в состояние `False`.

### `async wait(self) -> bool`

Блокирует выполнение до тех пор, пока флаг события не будет установлен в `True`. Повторно проверяет флаг после получения
каждого сообщения для предотвращения условий гонки.

## Примеры использования

1. **Синхронизация задач по расписанию.** Используйте `DistributedEvent` для координации начала задачи по обработке
   данных, которая должна запускаться одновременно на нескольких серверах.
2. **Оповещение о доступности новых данных.** Позволяет микросервисам ожидать появления новых данных в общем хранилище,
   прежде чем начать их обработку.
3. **Управление состоянием в распределённых системах.** Например, сигнализация о необходимости перезагрузки конфигурации
   или обновления программного обеспечения на всех узлах системы.