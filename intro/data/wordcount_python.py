from collections import defaultdict

if __name__ == '__main__':

    # defaultdict(int) : 초기 값을 0으로 세팅함
    word_count: dict[str, int] = defaultdict(int)

    with open("/spark_practice/data/words.txt", "r") as file:
        # words.txt 파일의 텍스트를 각각의 한 줄마다 띄어쓰기 기준으로 나누기
        # strip()으로 불필요한 스페이스 제외하고, split(" ")으로 띄어쓰기 단위로 나누기 -> 결과는 리스트 형식으로 저장됨
        for _, line in enumerate(file):
            words_each_line = line.strip().split(" ")

            # 위 리스트에서 특정 word가 있을 때마다 word_count 딕셔너리에 +1
            for _, word in enumerate(words_each_line):
                word_count[word] += 1

    for word, count in word_count.items():
        print(f"{word}: {count}")