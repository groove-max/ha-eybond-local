# Diagnostic Command Runner

## Статус документа

Техническое задание на реализацию простого диагностического инструмента для
разовых удалённых исследований пользовательских инверторов.

Связанная архитектура определения устройств описана в
`docs/INVERTER_CATALOG_REFACTOR.md`.

## Контекст

Для добавления или расширения поддержки конкретной модели стандартного Support
Archive иногда недостаточно. В процессе общения с владельцем устройства может
потребоваться:

- прочитать конкретный регистр или диапазон регистров;
- выполнить набор ASCII-команд;
- временно проверить другой драйвер, если автоматическое определение ошиблось;
- выполнить запрос с другим `devcode`, `collector_addr` или `device_addr`;
- записать значение в регистр и получить от пользователя наблюдение о результате;
- изменить один бит общего регистра, сохранив остальные биты;
- получить результат в виде текста и структурированного файла, который
  пользователь сможет передать разработчику.

Это разовый отладочный обмен с конкретным пользователем. Не требуется система
публикации, подписывания или централизованного распространения сценариев.

## Цель

Добавить в интеграцию runner, который:

1. принимает многострочный текстовый сценарий;
2. полностью разбирает и валидирует его до начала выполнения;
3. использует существующее соединение с коллектором;
4. позволяет временно переопределить драйвер и маршрут запроса;
5. последовательно выполняет команды;
6. возвращает человекочитаемый текст и структурированный результат;
7. не изменяет постоянные настройки config entry;
8. не требует добавлять новую capability, profile или schema для каждого
   диагностического запроса.

## Home Assistant action

Добавить action:

```text
eybond_local.run_diagnostic_commands
```

Поля:

- `entry_id` — обязательный config entry интеграции;
- `commands` — обязательное многострочное текстовое поле;
- `stop_on_error` — необязательное значение по умолчанию для сценария,
  по умолчанию `true`;
- `operation_timeout` — необязательный timeout одной транспортной операции в
  секундах. При отсутствии используется штатный timeout выбранного драйвера.

В `services.yaml` поле `commands` должно использовать multiline text selector.

Action должен поддерживать response data:

```json
{
  "success": true,
  "output": "human-readable multiline output",
  "results": [],
  "context": {},
  "started_at": "...",
  "finished_at": "...",
  "result_path": "...",
  "download_url": null
}
```

Отдельная frontend-панель в рамках этой задачи не требуется.

В описании action постоянно показывать предупреждение:

> Диагностические команды выполняются непосредственно на устройстве. Команды
> записи могут изменить его настройки.

Предупреждение является информационным и не требует отдельного подтверждения.

## Формат сценария

Сценарий является построчным текстом.

- Пустые строки игнорируются.
- Строки, первый непробельный символ которых `#`, являются комментариями.
- Числа принимаются в decimal и в формате с префиксом `0x`.
- Директивы применяются ко всему запуску и должны находиться до первой
  исполняемой команды.
- Неизвестные директивы и команды являются ошибкой.

Пример:

```text
# Временно проверяем SMG по нестандартному адресу
driver modbus_smg
devcode 1
collector_addr 0xFF
device_addr 4
stop_on_error true

read 171 14
read 643 2
write_bit 354 0 1
sleep 2000
read 354
```

## Директивы

### `driver`

```text
driver <driver_key>
```

Выбирает зарегистрированный драйвер для текущего запуска.

Если директива отсутствует, используется:

1. активный runtime driver, если он существует;
2. явно настроенный driver hint, если он не равен `auto`;
3. иначе сценарий отклоняется с просьбой явно указать `driver`.

Явно указанный driver может отличаться от автоматически определённого или
выбранного в config entry.

### `devcode`

```text
devcode <integer>
```

Временно переопределяет `ProbeTarget.devcode`.

### `collector_addr`

```text
collector_addr <integer>
```

Временно переопределяет `ProbeTarget.collector_addr`.

### `device_addr`

```text
device_addr <integer>
```

Временно переопределяет `ProbeTarget.device_addr`.

### `stop_on_error`

```text
stop_on_error <true|false>
```

Переопределяет одноимённое поле action для текущего сценария.

### `operation_timeout`

```text
operation_timeout <seconds>
```

Переопределяет timeout каждой отдельной транспортной операции. Это не общий
timeout сценария.

## Разрешение маршрута

Активный runtime и его `ProbeTarget` используются только как значения по
умолчанию.

При наличии директив значения выбираются независимо:

```text
effective_target = runtime_target + scenario_overrides
```

Overrides:

- существуют только в памяти на время запуска;
- не записываются в config entry;
- не изменяют persisted detection snapshot;
- не запускают обычное определение устройства;
- не меняют активную runtime surface.

Диагностика должна быть доступна даже при отсутствии обнаруженного inverter
runtime, если config entry имеет пригодное активное соединение с коллектором и
для построения `ProbeTarget` достаточно значений по умолчанию и overrides.

Runner получает драйвер через существующий registry и использует существующий
transport. Он не должен открывать отдельное TCP/UDP-соединение и не должен
реализовывать собственный EyeBond framing.

## Исполняемые команды

### `read`

```text
read <register> [count]
```

Читает holding register или последовательный диапазон holding registers.
`count` по умолчанию равен `1`.

Команда доступна для драйвера/транспорта, предоставляющего Modbus holding
register operations.

Результат должен содержать:

- исходную строку и её номер;
- effective route;
- начальный регистр;
- количество;
- decimal words;
- hexadecimal words;
- безопасное ASCII-представление, если оно получается из ответа;
- продолжительность;
- transport/protocol error.

Пример:

```text
[1] read 171 14
status: ok
duration_ms: 142
decimal: 8960 12336 12336 0 0 0 0 0 0 0 0 0 0 2
hex: 0x2300 0x3030 0x3030 0x0000 0x0000 0x0000 0x0000 0x0000 0x0000 0x0000 0x0000 0x0000 0x0000 0x0002
ascii: "#000"
```

### `write`

```text
write <register> <value> [value ...]
```

Записывает одно или несколько последовательных 16-битных значений, начиная с
указанного holding register.

Использовать существующий `ModbusSession.write_holding()`, который в текущей
реализации выполняет Modbus function `0x10` (Write Multiple Registers), в том
числе для одного значения.

Команда не выполняет автоматически:

- read-before-write;
- readback;
- восстановление предыдущего значения.

Если это требуется для исследования, операции должны быть явно указаны:

```text
read 354
write 354 1
sleep 2000
read 354
write 354 0
```

Дополнительный `allow_writes`, confirmation checkbox или иной блокер не нужен.
Наличие явной команды `write` считается намерением выполнить запись.

### `write_bit`

```text
write_bit <register> <bit_index> <0|1>
```

Изменяет один бит 16-битного holding register, сохраняя остальные биты.

Алгоритм является обязательным read-modify-write:

1. прочитать один register через `read_holding(register, 1)`;
2. проверить, что ответ содержит одно слово;
3. вычислить mask как `1 << bit_index`;
4. для значения `1` выполнить `current | mask`;
5. для значения `0` выполнить `current & ~mask`;
6. ограничить результат диапазоном `0x0000..0xFFFF`;
7. записать объединённое значение через существующий
   `ModbusSession.write_holding(register, [merged])`.

Это raw diagnostic-аналог уже существующей bitmask/RMW-семантики SMG
capabilities. Реализацию следует переиспользовать или вынести в общий helper,
чтобы не получить две различающиеся реализации RMW.

Команда должна возвращать как минимум:

```json
{
  "kind": "modbus_write_bit",
  "request": {
    "register": 354,
    "bit_index": 0,
    "bit_value": 1,
    "mask": "0x0001"
  },
  "before": {
    "decimal": 43982,
    "hex": "0xABCE"
  },
  "written": {
    "decimal": 43983,
    "hex": "0xABCF"
  },
  "status": "ok"
}
```

Предварительное чтение является частью самой RMW-операции, а не
дополнительной защитой. Автоматический post-write readback не выполняется.
Если он нужен, в сценарий добавляется явный `read`.

Допустимые значения:

- `bit_index`: `0..15`;
- `bit_value`: только `0` или `1`.

Если предварительное чтение завершилось ошибкой или вернуло пустой ответ,
запись не выполняется.

### `ascii`

```text
ascii <command and optional arguments>
```

Передаёт raw ASCII-команду через выбранный PI driver.

Примеры:

```text
driver pi30
ascii QPI
ascii QPIRI
ascii QPIGS
ascii QFLAG
ascii QMOD
```

Требования:

- использовать штатный framing/checksum выбранного драйвера;
- использовать effective route текущего диагностического запуска;
- вернуть raw response;
- по возможности отдельно вернуть payload без framing;
- не требовать наличия команды в catalog probe actions;
- не интерпретировать неизвестную команду как capability.

### `sleep`

```text
sleep <milliseconds>
```

Приостанавливает последовательное выполнение на указанное время.

Runner не должен вводить произвольный небольшой верхний предел. Значение должно
быть неотрицательным и технически представимым для `asyncio.sleep`.

## Совместимость команд и драйверов

Сценарий полностью валидируется до выполнения первой команды.

Минимальная матрица:

| Driver | `read` | `write` | `write_bit` | `ascii` |
|---|---:|---:|---:|---:|
| `modbus_smg` | yes | yes | yes | no |
| `pi30` | no | no | no | yes |
| `pi18` | no | no | no | yes |

Матрица не должна быть жёстко размазана по parser-коду. Желательно описать
диагностические primitives интерфейсом или capabilities выбранного target.

При несовместимости весь сценарий отклоняется до выполнения:

```text
line 7: command 'write_bit' is not supported by driver 'pi30'
```

## Валидация

Весь сценарий должен быть распарсен и проверен до первой transport operation.

Проверять:

- директивы находятся до исполняемых команд;
- driver существует в registry;
- для effective target доступны все обязательные поля;
- адреса и значения находятся в диапазоне, поддерживаемом соответствующим
  protocol primitive;
- `read count` положительный;
- `write` содержит хотя бы одно значение;
- `write_bit bit_index` находится в `0..15`;
- `write_bit bit_value` равен `0` или `1`;
- `sleep` неотрицательный;
- `operation_timeout` положительный;
- команда поддерживается выбранным driver/target;
- неизвестные токены не игнорируются.

Ошибки должны содержать исходный номер строки:

```text
line 3: unknown command 'reads'
line 5: register must be between 0 and 65535
line 8: bit index must be between 0 and 15
line 10: command 'ascii' is not supported by driver 'modbus_smg'
```

## Ограничения

Не вводить произвольные продуктовые лимиты на:

- общее количество команд;
- общее время сценария;
- продолжительность `sleep`;
- количество прочитанных или записанных регистров сверх ограничений самого
  протокола и существующего transport primitive.

Runner обязан соблюдать реальные технические ограничения используемого
протокола, framing и transport API. Например, допустимый размер одного Modbus
запроса определяется реализацией Modbus, а не отдельной политикой runner.

Оставить следующие эксплуатационные ограничения:

- команды выполняются строго последовательно;
- для одного config entry одновременно выполняется не более одного сценария;
- каждая transport operation имеет timeout;
- `stop_on_error=true` прекращает выполнение после первой runtime-ошибки;
- `stop_on_error=false` записывает ошибку шага и продолжает со следующей
  независимой командой;
- при unload config entry активный runner отменяется;
- при потере соединения возвращается понятная ошибка;
- отмена не должна оставлять lock запуска захваченным.

## Архитектура

Предлагаемое разделение:

```text
custom_components/eybond_local/
  support/
    diagnostic_commands.py
    diagnostic_runner.py
    diagnostic_export.py
  services.py
  services.yaml
```

### `diagnostic_commands.py`

Содержит:

- независимые от Home Assistant модели директив и команд;
- parser;
- синтаксическую валидацию;
- форматирование parse errors с номерами строк.

### `diagnostic_runner.py`

Содержит:

- разрешение effective driver и `ProbeTarget`;
- проверку поддерживаемых primitives;
- последовательное выполнение;
- per-entry lock;
- timeout и cancellation;
- сбор структурированных результатов;
- text rendering.

### Диагностический target

Runner не должен использовать `async_write_capability()` для raw-команд,
поскольку диагностируемого регистра может ещё не быть в profile.

Допускается добавить минимальный внутренний интерфейс:

```python
class DiagnosticCommandTarget(Protocol):
    async def read_holding(self, register: int, count: int) -> list[int]: ...
    async def write_holding(self, register: int, values: list[int]) -> None: ...
    async def send_ascii(self, command: str) -> str: ...
```

Конкретный target предоставляет только поддерживаемые primitives. Для SMG
следует переиспользовать `ModbusSession`. Для PI30/PI18 следует переиспользовать
существующий command framing и transport execution.

Если для повторного использования bitmask write потребуется рефакторинг,
вынести чистую операцию merge отдельно:

```python
def merge_register_bit(current: int, bit_index: int, bit_value: int) -> int:
    ...
```

Transport-level RMW должен использовать этот helper и существующие
`read_holding`/`write_holding`.

## Результат выполнения

Каждый шаг должен возвращать:

```json
{
  "line": 7,
  "command": "read 171 14",
  "kind": "modbus_read",
  "status": "ok",
  "duration_ms": 142,
  "request": {},
  "response": {},
  "error": null
}
```

Общий context:

```json
{
  "integration_version": "...",
  "entry_id": "...",
  "selected_driver_key": "modbus_smg",
  "driver_source": "scenario_override",
  "probe_target": {
    "devcode": 1,
    "collector_addr": 255,
    "device_addr": 4
  },
  "catalog_detection": {
    "candidate_keys": [],
    "surface_key": "",
    "evidence_fingerprint": ""
  }
}
```

`catalog_detection` является диагностическим контекстом активного runtime и не
должен запрещать выполнение с другим driver или target.

## Приватность результата

Shareable export не должен включать:

- collector IP или remote IP;
- collector PN;
- inverter serial;
- account identifiers;
- credentials, token или secret;
- полный config entry payload.

Необходимо учитывать, что raw прочитанные регистры или ASCII-ответы сами могут
содержать серийный номер. Runner не должен удалять запрошенный raw ответ из
основного локального результата, иначе исследование может потерять смысл.

Поэтому экспорт должен явно разделять:

- локальный raw result;
- shareable result с применением существующих redaction helpers к известным
  serial/identity полям и текстовым ответам.

Action response пользователю может содержать raw результат, поскольку он
выполнил запрос на своём устройстве. `download_url`, предназначенный для
передачи разработчику, должен указывать на shareable export.

## Экспорт

После выполнения сохранить JSON и TXT в:

```text
/config/eybond_local/diagnostic_runs/
```

Рекомендуемые имена:

```text
diagnostic_<entry_id>_<timestamp>.json
diagnostic_<entry_id>_<timestamp>.txt
diagnostic_<entry_id>_<timestamp>.share.json
```

Если существующий механизм публикации support artifacts через `/local` можно
переиспользовать без существенного усложнения, вернуть `download_url`. Иначе
достаточно `result_path` и response data.

Диагностический результат не нужно автоматически добавлять в обычный Support
Archive.

## Тесты

Добавить unit-тесты как минимум для:

- пустых строк и комментариев;
- decimal и hexadecimal чисел;
- всех директив;
- директивы после первой команды;
- парсинга `read`, `write`, `write_bit`, `ascii`, `sleep`;
- неизвестной команды;
- неверного количества аргументов;
- полного preflight до первой transport operation;
- driver override относительно активного runtime;
- `device_addr`, `devcode` и `collector_addr` overrides;
- запуска без обнаруженного inverter runtime при доступном collector transport;
- несовместимой команды и driver;
- последовательного выполнения;
- `stop_on_error=true` и `false`;
- operation timeout;
- per-entry lock;
- cancellation/unload;
- записи одного и нескольких holding registers;
- `write` без автоматического pre-read/readback;
- `write_bit` для установки и сброса каждого граничного bit index;
- сохранения остальных 15 бит при `write_bit`;
- отсутствия записи при ошибке предварительного чтения `write_bit`;
- отсутствия автоматического post-read после `write_bit`;
- raw и shareable exports;
- отсутствия известных идентификаторов в shareable export;
- Home Assistant action response.

Для transport tests использовать существующие fake collector/fixture transport
механизмы проекта.

## Не входит в задачу

Не реализовывать:

- custom frontend panel;
- shell или Python console;
- произвольные TCP/UDP payloads;
- циклы, условия, переменные и expressions;
- подпись или checksum сценариев;
- библиотеку опубликованных сценариев;
- автоматический импорт результата в inverter catalog;
- автоматическое изменение profile или register schema;
- автоматическое восстановление записанного значения;
- denylist регистров;
- отдельный permission/confirmation flow для write-команд;
- постоянное изменение driver hint или probe target config entry.

## Критерии приёмки

Задача выполнена, если:

1. В Home Assistant доступен action с многострочным полем сценария.
2. Активный driver и route используются как defaults, но могут быть временно
   переопределены сценарием.
3. Диагностика может работать при неуспешном normal inverter detection, если
   соединение с collector доступно.
4. На SMG можно читать и записывать произвольные holding registers через
   существующий Modbus transport.
5. `write_bit` меняет только указанный бит и сохраняет остальные.
6. На PI30 и PI18 можно выполнять raw ASCII-команды через штатный framing.
7. Все команды валидируются до начала выполнения.
8. Ошибки содержат номера исходных строк.
9. Результат возвращается как многострочный текст и структурированный JSON.
10. Постоянные настройки config entry не изменяются.
11. Shareable export не содержит известных персональных идентификаторов.
12. Все новые и существующие релевантные тесты проходят.

## Ограничение области изменений

В рабочем дереве уже находится большой незакоммиченный рефакторинг каталога.
Реализация должна сохранять существующие изменения, не откатывать их и не
переписывать несвязанные файлы.
