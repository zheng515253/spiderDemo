import os


class ChangePicName:
    def __init__(self):
        self.root_path = os.path.dirname(os.path.realpath(__file__))

    def file_name(self, user_dir):
        file_list = list()
        for root, dirs, files in os.walk(user_dir):
            for file in files:
                # if os.path.splitext(file)[1] == '.txt':
                file_list.append(os.path.join(root, file))
        return file_list

    def change_name(self):
        path = self.root_path + '/' + 'old_pic'
        new_path = self.root_path + '/' + 'new_pic'
        file_list = self.file_name(path)
        a = 1
        for file in file_list:
            a += 1
            file_path = file.replace("\\", "/")
            with open(file_path, 'rb') as f:
                pic_content = f.read()
                new_file_name = new_path + '/' + str(a) + '.jpg'
                with open(new_file_name, 'wb') as t:
                    t.write(pic_content)

    def run(self):
        self.change_name()


if __name__ == '__main__':
    change_pic_name = ChangePicName()
    change_pic_name.run()