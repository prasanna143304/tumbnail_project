# thumbnailer.py
import os
import io
import sys
from multiprocessing import Process, Queue, Value, cpu_count
from PIL import Image

PRODUCER_DIR = "producer"
CONSUMER_DIR = "consumer"
THUMBNAIL_SIZE = (200, 200)  # adjust as needed
NUM_CONSUMERS = max(1, cpu_count() - 1)  # default, can be changed

def producer_task(q: Queue, producer_dir: str):
    """
    Producer: reads each image file, creates a thumbnail (in memory),
    and puts (orig_name, image_bytes) into the queue.
    After finishing, put NUM_CONSUMERS sentinel values (None).
    """
    supported = (".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp")
    file_list = sorted([
        f for f in os.listdir(producer_dir)
        if os.path.isfile(os.path.join(producer_dir, f)) and f.lower().endswith(supported)
    ])

    for fname in file_list:
        path = os.path.join(producer_dir, fname)
        try:
            with Image.open(path) as img:
                img = img.convert("RGB")  # ensure consistent format
                img.thumbnail(THUMBNAIL_SIZE)
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85)
                buf.seek(0)
                q.put((fname, buf.read()))
                print(f"[Producer] queued thumbnail for: {fname}")
        except Exception as e:
            print(f"[Producer] ERROR processing {fname}: {e}", file=sys.stderr)

    # signal consumers to stop
    for _ in range(NUM_CONSUMERS):
        q.put(None)
    print("[Producer] finished; sent sentinel(s).")

def consumer_task(q: Queue, consumer_dir: str, counter: Value):
    """
    Consumer: waits for (fname, bytes) from the queue and writes them to disk.
    Increments shared 'counter' on success.
    Exits on sentinel (None).
    """
    os.makedirs(consumer_dir, exist_ok=True)
    while True:
        item = q.get()
        if item is None:
            print("[Consumer] received sentinel; exiting.")
            break
        try:
            orig_name, img_bytes = item
            name_wo_ext = os.path.splitext(orig_name)[0]
            out_name = f"{name_wo_ext}-thumbnail.jpg"
            out_path = os.path.join(consumer_dir, out_name)
            with open(out_path, "wb") as f:
                f.write(img_bytes)
            with counter.get_lock():
                counter.value += 1
            print(f"[Consumer] saved: {out_name}")
        except Exception as e:
            print(f"[Consumer] ERROR saving thumbnail: {e}", file=sys.stderr)

def main():
    producer_dir = PRODUCER_DIR
    consumer_dir = CONSUMER_DIR

    if not os.path.isdir(producer_dir):
        print(f"Producer directory '{producer_dir}' does not exist. Create it and add images.", file=sys.stderr)
        return

    q = Queue(maxsize=NUM_CONSUMERS * 4)
    counter = Value('i', 0)

    # Start producers and consumers
    p = Process(target=producer_task, args=(q, producer_dir), name="Producer")
    consumers = []
    for i in range(NUM_CONSUMERS):
        c = Process(target=consumer_task, args=(q, consumer_dir, counter), name=f"Consumer-{i+1}")
        consumers.append(c)

    p.start()
    for c in consumers:
        c.start()

    # Wait for processes to finish
    p.join()
    for c in consumers:
        c.join()

    print(f"All done. Thumbnails converted and saved: {counter.value}")

if __name__ == "__main__":
    main()
